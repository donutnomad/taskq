package membership

import "errors"

var ErrUnknownService = errors.New("service not tracked by Monitor")
var ErrInsufficientHosts = errors.New("not enough hosts to serve the request")
var ErrListenerAlreadyExist = errors.New("listener already exist for the service")
var ErrResolverStopped = errors.New("service resolver stopped")
var ErrIncorrectAddressFormat = errors.New("incorrect address format")
