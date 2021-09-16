package utils

import (
	"testing"
)

func TestCopyDir(t *testing.T) {
	srcPath := "D:\\github\\stardb\\testFile\\tmp1"
	dstPath := "D:\\github\\stardb\\testFile\\tmp2"

	err := CopyDir(srcPath, dstPath)
	if err != nil{
		t.Error("CopyDir err:", err)
	}
}

func TestCopyFile(t *testing.T) {
	srcFile := "D:\\github\\stardb\\testFile\\tmp1\\aaa.txt"
	dstFile := "D:\\github\\stardb\\testFile\\tmp1\\bbb.txt"

	err := CopyFile(srcFile, dstFile)
	if err != nil{
		t.Error("CopyFile err:", err)
	}
}