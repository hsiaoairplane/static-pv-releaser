package main

import (
	"context"
	"flag"
	"os"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
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
	if !hasBindingConflict(&pvc) {
		return ctrl.Result{}, nil
	}

	var pvList corev1.PersistentVolumeList
	if err := r.List(ctx, &pvList); err != nil {
		return ctrl.Result{}, err
	}

	for _, pv := range pvList.Items {
		if pv.Spec.ClaimRef == nil {
			continue
		}
		if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
			continue
		}
		if pv.Status.Phase != corev1.VolumeReleased {
			continue
		}

		// If PV is bound to another PVC
		if pv.Spec.ClaimRef.Name != pvc.Name ||
			pv.Spec.ClaimRef.Namespace != pvc.Namespace {

			logger.Info("Releasing PV claimRef",
				"pv", pv.Name,
				"oldPVC", pv.Spec.ClaimRef.Namespace+"/"+pv.Spec.ClaimRef.Name)

			patch := client.MergeFrom(pv.DeepCopy())
			pv.Spec.ClaimRef.UID = ""
			pv.Spec.ClaimRef.ResourceVersion = ""

			if err := r.Patch(ctx, &pv, patch); err != nil {
				return ctrl.Result{}, err
			}

			// Requeue quickly to let binding retry
			return ctrl.Result{Requeue: true}, nil
		}
	}

	return ctrl.Result{}, nil
}

func hasBindingConflict(pvc *corev1.PersistentVolumeClaim) bool {
	for _, cond := range pvc.Status.Conditions {
		if cond.Type == corev1.PersistentVolumeClaimResizing {
			continue
		}
		if strings.Contains(cond.Message, "already bound") {
			return true
		}
	}
	return false
}

func (r *PVCReclaimerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	pvcPredicate := predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return true
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldPVC := e.ObjectOld.(*corev1.PersistentVolumeClaim)
			newPVC := e.ObjectNew.(*corev1.PersistentVolumeClaim)

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
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	flag.BoolVar(&enableLeaderElection, "leader-elect", true, "Enable leader election for controller manager")
	flag.StringVar(&namespace, "namespace", "cdp", "Deploy namespace")

	flag.Parse()

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                  scheme,
		LeaderElection:          true,
		LeaderElectionID:        "pv-releaser",
		LeaderElectionNamespace: namespace,
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
