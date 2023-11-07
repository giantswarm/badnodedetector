module github.com/giantswarm/badnodedetector/v2

go 1.16

require (
	github.com/giantswarm/microerror v0.4.0
	github.com/giantswarm/micrologger v0.6.0
	github.com/google/go-cmp v0.6.0
	k8s.io/api v0.22.17
	k8s.io/apimachinery v0.22.17
	sigs.k8s.io/controller-runtime v0.10.3
)

replace (
	github.com/coreos/etcd v3.3.13+incompatible => github.com/coreos/etcd v3.3.25+incompatible
	github.com/dgrijalva/jwt-go => github.com/dgrijalva/jwt-go/v4 v4.0.0-preview1
	github.com/prometheus/client_golang => github.com/prometheus/client_golang v1.17.0
	golang.org/x/net => golang.org/x/net v0.17.0
)
