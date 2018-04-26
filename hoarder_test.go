// +build unit

package ifs

import (
	"testing"
)

func TestHoarder_GetCacheFileName(t *testing.T) {

	h := Hoarder{
	}

	p1 := h.GetCacheFileName()

	if p1 != "0" {
		PrintTestError(t,"Cache File Name doesnt match ", p1, 0)
	}

	p2 := h.GetCacheFileName()

	if p2 != "1" {
		PrintTestError(t,"Cache File Name doesnt match ", p2, 0)
	}

	p3 := h.GetCacheFileName()

	if p3 != "2" {
		PrintTestError(t,"Cache File Name doesnt match ", p3, 0)
	}

	p4 := h.GetCacheFileName()

	if p4 != "3" {
		PrintTestError(t,"Cache File Name doesnt match ", p4, 0)
	}

	p5 := h.GetCacheFileName()

	if p5 != "4" {
		PrintTestError(t,"Cache File Name doesnt match ", p5, 0)
	}

}
