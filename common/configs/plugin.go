package configs

// General description of any user-defined
// plugin configuration section
type PluginConfig struct {
	Type string                 `yaml:"type"`
	Args map[string]interface{} `yaml:",inline"`
}

// func PluginConfigsUpdate(source *PluginConfig, update *PluginConfig) {
// 	for k, v := range *update {
// 		(*source)[k] = v
// 	}
// }
