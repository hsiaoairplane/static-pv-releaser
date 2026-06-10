package main

import (
	"context"
	"flag"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	storagehelpers "k8s.io/component-helpers/storage/volume"
	"k8s.io/kubernetes/pkg/controller/volume/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	staticPVReleaserSuccessTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "static_pv_releaser_success_total",
			Help: "Total number of PersistentVolumes successfully released by the static PV releaser",
		},
		[]string{"pv"},
	)

	staticPVReleaserFailureTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "static_pv_releaser_failure_total",
			Help: "Total number of PersistentVolumes failed to be released by the static PV releaser",
		},
		[]string{"pv"},
	)
)

type PVCReclaimerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

func (r *PVCReclaimerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var pvc corev1.PersistentVolumeClaim
	if err := r.Get(ctx, req.NamespacedName, &pvc); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	// Only care about Pending PVCs
	if pvc.Status.Phase != corev1.ClaimPending {
		return ctrl.Result{}, nil
	}

	// Look for binding failure events via annotations
	hasConflict, err := r.hasBindingConflictEvent(ctx, &pvc)
	if err != nil {
		return ctrl.Result{}, err
	}

	if !hasConflict {
		return ctrl.Result{}, nil
	}

	var pvList corev1.PersistentVolumeList
	if err := r.List(ctx, &pvList); err != nil {
		return ctrl.Result{}, err
	}

	for _, pv := range pvList.Items {
		// Check Persistent Volume (PV) Spec
		if pv.Spec.ClaimRef == nil || // No ClaimRef
			pv.Spec.ClaimRef.Namespace != pvc.Namespace || // Different PVC Namespace
			pv.Spec.ClaimRef.Name != pvc.Name || // Different PVC Name
			pv.Spec.ClaimRef.UID == pvc.UID || // Same PVC UID
			pv.Spec.ClaimRef.ResourceVersion == "" || // No ResourceVersion to compare
			pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain || // Not Retain policy
			pv.Spec.NFS == nil || // Not NFS volume
			pv.Spec.NFS.Server == "" || // Missing NFS server
			pv.Spec.NFS.Path == "" { // Missing NFS path
			continue
		}
		// Check Persistent Volume (PV) Status
		if pv.Status.Phase != corev1.VolumeReleased {
			continue
		}

		// If PV is bound to previous PVC (pv.Spec.ClaimRef.UID != pvc.UID), clear ClaimRef.UID and ClaimRef.ResourceVersion
		logger.Info("releasing PV claimRef",
			"pv", pv.Name,
			"pvc", pv.Spec.ClaimRef.Namespace+"/"+pv.Spec.ClaimRef.Name,
			"uid", pv.Spec.ClaimRef.UID)

		patch := client.MergeFrom(pv.DeepCopy())
		pv.Spec.ClaimRef.UID = ""
		pv.Spec.ClaimRef.ResourceVersion = ""

		if err := r.Patch(ctx, &pv, patch); err != nil {
			// Record failure metric
			staticPVReleaserFailureTotal.WithLabelValues(pv.Name).Inc()

			// Log failure
			logger.Error(err, "failed to release PV claimRef",
				"pv", pv.Name,
				"pvc", pv.Spec.ClaimRef.Namespace+"/"+pv.Spec.ClaimRef.Name,
				"uid", pv.Spec.ClaimRef.UID)

			return ctrl.Result{}, err
		}

		// Record success metric
		staticPVReleaserSuccessTotal.WithLabelValues(pv.Name).Inc()

		// Log success
		logger.Info("successfully released PV claimRef",
			"pv", pv.Name,
			"pvc", pv.Spec.ClaimRef.Namespace+"/"+pv.Spec.ClaimRef.Name,
			"uid", pv.Spec.ClaimRef.UID)

		return ctrl.Result{}, nil
	}

	return ctrl.Result{}, nil
}

func (r *PVCReclaimerReconciler) hasBindingConflictEvent(
	ctx context.Context,
	pvc *corev1.PersistentVolumeClaim,
) (bool, error) {

	var eventList corev1.EventList
	if err := r.List(ctx, &eventList, client.InNamespace(pvc.Namespace)); err != nil {
		return false, err
	}

	for _, ev := range eventList.Items {
		if ev.InvolvedObject.Kind != "PersistentVolumeClaim" {
			continue
		}
		if ev.InvolvedObject.Namespace != pvc.Namespace {
			continue
		}
		if ev.InvolvedObject.Name != pvc.Name {
			continue
		}
		if ev.Reason != events.FailedBinding {
			continue
		}
		if strings.Contains(ev.Message, "already bound") {
			return true, nil
		}
	}

	return false, nil
}

func (r *PVCReclaimerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	pvcPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			pvc := e.Object.(*corev1.PersistentVolumeClaim)
			return !metav1.HasAnnotation(pvc.ObjectMeta, storagehelpers.AnnBoundByController)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPVC := e.ObjectOld.(*corev1.PersistentVolumeClaim)
			newPVC := e.ObjectNew.(*corev1.PersistentVolumeClaim)

			// Skip controller-bound PVCs
			if metav1.HasAnnotation(newPVC.ObjectMeta, storagehelpers.AnnBoundByController) {
				return false
			}

			// Transition into Pending
			return oldPVC.Status.Phase != corev1.ClaimPending &&
				newPVC.Status.Phase == corev1.ClaimPending
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return false
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return false
		},
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.PersistentVolumeClaim{}).
		WithEventFilter(pvcPredicate).
		Complete(r)
}

var (
	scheme               = runtime.NewScheme()
	setupLog             = ctrl.Log.WithName("setup")
	enableLeaderElection bool
	namespace            string
	probeAddr            string
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	metrics.Registry.MustRegister(staticPVReleaserSuccessTotal, staticPVReleaserFailureTotal)
}

func main() {
	flag.BoolVar(&enableLeaderElection, "leader-elect", true, "Enable leader election for controller manager")
	flag.StringVar(&namespace, "namespace", "cdp", "Deploy namespace")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the health probe endpoint binds to")

	flag.Parse()

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		LeaderElection:          enableLeaderElection,
		LeaderElectionID:        "static-pv-releaser",
		LeaderElectionNamespace: namespace,
		HealthProbeBindAddress:  probeAddr,
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	if err = (&PVCReclaimerReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "PV Releaser")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")

	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}
