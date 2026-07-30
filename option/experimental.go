package option

import "github.com/sagernet/sing/common/json/badoption"

type ExperimentalOptions struct {
	CacheFile     *CacheFileOptions     `json:"cache_file,omitempty"`
	ClashAPI      *ClashAPIOptions      `json:"clash_api,omitempty"`
	V2RayAPI      *V2RayAPIOptions      `json:"v2ray_api,omitempty"`
	OpenTelemetry *OpenTelemetryOptions `json:"opentelemetry,omitempty"`
	Debug         *DebugOptions         `json:"debug,omitempty"`
}

type OpenTelemetryOptions struct {
	Enabled            bool                      `json:"enabled,omitempty"`
	Endpoint           string                    `json:"endpoint,omitempty"`
	Protocol           string                    `json:"protocol,omitempty"`
	Headers            map[string]string         `json:"headers,omitempty"`
	Compression        string                    `json:"compression,omitempty"`
	Timeout            badoption.Duration        `json:"timeout,omitempty"`
	ActiveTimeout      badoption.Duration        `json:"active_timeout,omitempty"`
	Batch              OpenTelemetryBatchOptions `json:"batch,omitempty"`
	TLS                OpenTelemetryTLSOptions   `json:"tls,omitempty"`
	ResourceAttributes map[string]string         `json:"resource_attributes,omitempty"`
}

type OpenTelemetryBatchOptions struct {
	ScheduleDelay      badoption.Duration `json:"schedule_delay,omitempty"`
	ExportTimeout      badoption.Duration `json:"export_timeout,omitempty"`
	MaxQueueSize       int                `json:"max_queue_size,omitempty"`
	MaxExportBatchSize int                `json:"max_export_batch_size,omitempty"`
}

type OpenTelemetryTLSOptions struct {
	CACertificate      string `json:"ca_certificate,omitempty"`
	ClientCertificate  string `json:"client_certificate,omitempty"`
	ClientKey          string `json:"client_key,omitempty"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify,omitempty"`
}

type CacheFileOptions struct {
	Enabled     bool               `json:"enabled,omitempty"`
	Path        string             `json:"path,omitempty"`
	CacheID     string             `json:"cache_id,omitempty"`
	StoreFakeIP bool               `json:"store_fakeip,omitempty"`
	StoreRDRC   bool               `json:"store_rdrc,omitempty" schema:"omit"`
	RDRCTimeout badoption.Duration `json:"rdrc_timeout,omitempty"`
	StoreDNS    bool               `json:"store_dns,omitempty"`
}

type ClashAPIOptions struct {
	ExternalController               string                     `json:"external_controller,omitempty"`
	ExternalUI                       string                     `json:"external_ui,omitempty"`
	ExternalUIDownloadURL            string                     `json:"external_ui_download_url,omitempty"`
	ExternalUIDownloadDetour         string                     `json:"external_ui_download_detour,omitempty" reference:"outbound"`
	Secret                           string                     `json:"secret,omitempty"`
	DefaultMode                      string                     `json:"default_mode,omitempty"`
	ModeList                         []string                   `json:"-"`
	AccessControlAllowOrigin         badoption.Listable[string] `json:"access_control_allow_origin,omitempty"`
	AccessControlAllowPrivateNetwork bool                       `json:"access_control_allow_private_network,omitempty"`

	// Deprecated: migrated to global cache file
	CacheFile string `json:"cache_file,omitempty" schema:"omit"`
	// Deprecated: migrated to global cache file
	CacheID string `json:"cache_id,omitempty" schema:"omit"`
	// Deprecated: migrated to global cache file
	StoreMode bool `json:"store_mode,omitempty" schema:"omit"`
	// Deprecated: migrated to global cache file
	StoreSelected bool `json:"store_selected,omitempty" schema:"omit"`
	// Deprecated: migrated to global cache file
	StoreFakeIP bool `json:"store_fakeip,omitempty" schema:"omit"`
}

type V2RayAPIOptions struct {
	Listen string                    `json:"listen,omitempty"`
	Stats  *V2RayStatsServiceOptions `json:"stats,omitempty"`
}

type V2RayStatsServiceOptions struct {
	Enabled   bool     `json:"enabled,omitempty"`
	Inbounds  []string `json:"inbounds,omitempty"`
	Outbounds []string `json:"outbounds,omitempty"`
	Users     []string `json:"users,omitempty"`
}
