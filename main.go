package controllers

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

type PVCReclaimerReconciler struct {
	client.Client
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
