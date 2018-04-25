// +build linux darwin

package oshelper

import (
	"syscall"

	"code.cloudfoundry.org/csiplugin"
)

type osHelper struct {
}

func NewOsHelper() csiplugin.OsHelper {
	return &osHelper{}
}

func (o *osHelper) Umask(mask int) (oldmask int) {
	return syscall.Umask(mask)
}
