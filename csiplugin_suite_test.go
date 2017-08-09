package csiplugin_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestCsiplugin(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Csiplugin Suite")
}
