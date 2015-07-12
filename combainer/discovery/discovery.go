package discovery

type Discovery interface {
	GetHosts() ([]string, error)
}

type CloudHostsDelegate func() ([]string, error)

type httpDiscovery struct {
	f CloudHostsDelegate
}

func NewHTTPDiscovery(f CloudHostsDelegate) Discovery {
	return &httpDiscovery{
		f: f,
	}
}

func (h *httpDiscovery) GetHosts() ([]string, error) {
	return h.f()
}
