package cri

import (
	"context"
	"fmt"
	"runtime"

	"github.com/kubeclipper/kubeclipper/pkg/component"
	componentcommon "github.com/kubeclipper/kubeclipper/pkg/component/common"
	deliveryapis "github.com/kubeclipper/kubeclipper/pkg/delivery/apis"
	deliveryfetcher "github.com/kubeclipper/kubeclipper/pkg/delivery/fetcher"
	"github.com/kubeclipper/kubeclipper/pkg/simple/downloader"
	"github.com/kubeclipper/kubeclipper/pkg/utils/cmdutil"
)

func applyResolvedRuntime(ctx context.Context, base *Base, name string) {
	if resolved, ok := componentcommon.FindResolvedComponent(component.GetResolvedArtifactPlan(ctx), "cri", name, base.Version); ok {
		base.Arch = resolved.Arch
		base.Transport = resolved.Transport
		base.Contents = resolved.Contents
	}
}

func downloadAndUnpackResolvedRuntimeConfigs(ctx context.Context, base Base, name string, dryRun bool) error {
	contents := base.Contents
	if len(contents) == 0 {
		contents = []deliveryapis.ArtifactContent{{Name: deliveryapis.ContentConfigs, File: downloader.ConfigFilename}}
	}
	result, err := deliveryfetcher.FetchComponent(ctx, runtime.GOARCH, deliveryapis.ResolvedComponent{
		Kind: "cri", Name: name, Version: base.Version, Arch: base.Arch,
		Transport: base.Transport, Contents: contents,
	}, dryRun)
	if err != nil {
		return err
	}
	configs := result.Files[deliveryapis.ContentConfigs]
	if configs == "" {
		return fmt.Errorf("resolved %s configs content is missing", name)
	}
	_, err = cmdutil.RunCmdWithContext(ctx, dryRun, "bash", "-c", fmt.Sprintf("tar -zxvf %s -C /", configs))
	return err
}
