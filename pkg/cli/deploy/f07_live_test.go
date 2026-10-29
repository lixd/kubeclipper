package deploy

import (
 "context"
 "encoding/json"
 "os"
 "reflect"
 "testing"

 "github.com/kubeclipper/kubeclipper/pkg/cli/config"
 "github.com/kubeclipper/kubeclipper/pkg/constatns"
 deliveryregistry "github.com/kubeclipper/kubeclipper/pkg/delivery/registry"
 v1 "github.com/kubeclipper/kubeclipper/pkg/scheme/core/v1"
 "github.com/kubeclipper/kubeclipper/pkg/simple/client/kc"
)

// TestF07LiveSeed is a test harness for the unchanged production seed routine against the live API.
func TestF07LiveSeed(t *testing.T) {
 path:=os.Getenv("F07_LIVE_REGISTRY_CONFIG");if path=="" {t.Skip("live configuration required")}
 data,err:=os.ReadFile(path);if err!=nil {t.Fatal(err)}
 var cfg deliveryregistry.Config;if err=json.Unmarshal(data,&cfg);err!=nil {t.Fatal(err)}
 conf,err:=config.TryLoadFromFile("/root/.kc/config");if err!=nil {t.Fatal(err)}
 client,err:=kc.FromConfig(*conf);if err!=nil {t.Fatal(err)}
 if err:=uploadDefaultRegistry(client,&cfg);err!=nil {t.Fatal(err)}
 result,err:=client.DescribeRegistries(context.Background(),constatns.DefaultImageRegistryName);if err!=nil {t.Fatal(err)}
 host,prefix:=deliveryregistry.SplitPrefix(cfg.Registry)
 expected:=v1.RegistrySpec{Host:host,RepositoryPrefix:prefix,Scheme:cfg.Scheme,CA:cfg.CA,SkipVerify:cfg.SkipTLSVerify,RegistryAuth:&v1.RegistryAuth{Username:cfg.Username,Password:cfg.Password}}
 expected.RegistryAuth.Password="******"
 if len(result.Items)!=1 || !reflect.DeepEqual(result.Items[0].RegistrySpec,expected) {t.Fatal("live default seed did not retain the exact endpoint, prefix, TLS and authentication configuration")}
}
