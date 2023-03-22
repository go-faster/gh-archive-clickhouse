#!/bin/sh

echo "${KUBE}" | base64 -d > "${KUBECONFIG}"
