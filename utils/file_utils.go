package utils

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
)

// Exist 校验目录或文件是否存在
func Exist(path string) bool {
	if _, err := os.Stat(path); os.IsNotExist(err){
		return false
	}

	return true
}

//拷贝目录
func CopyDir(src string, dst string) error{
	var (
		err error
		dir []os.FileInfo
		srcInfo os.FileInfo
	)

	//判断原目录是否存在
	if srcInfo, err = os.Stat(src); err != nil{
		return err
	}
	//创建目标目录
	if err = os.MkdirAll(dst, srcInfo.Mode()); err != nil{
		return err
	}
	//读取src下的文件和目录
	if dir, err = ioutil.ReadDir(src); err != nil{
		return err
	}

	for _, fd := range dir{
		srcPath := fmt.Sprintf("%s%s%s", src, string(os.PathSeparator), fd.Name())//path.Join(src, fd.Name())
		dstPath := fmt.Sprintf("%s%s%s", dst, string(os.PathSeparator), fd.Name())//path.Join(dst, fd.Name())

		if fd.IsDir(){
			if err = CopyDir(srcPath, dstPath); err != nil{  //递归复制
				return err
			}
		} else{
			if err = CopyFile(srcPath, dstPath); err != nil{
				return err
			}
		}
	}

	return nil
}

//CopyFile 拷贝文件
func CopyFile(src string, dst string) error {
	var(
		err error
		srcFile *os.File
		dstFile *os.File
		srcInfo os.FileInfo
	)

	if srcFile, err = os.Open(src); err != nil{
		return err
	}

	defer srcFile.Close()

	if dstFile, err = os.Create(dst); err != nil{
		return err
	}

	defer dstFile.Close()

	if _, err = io.Copy(dstFile, srcFile); err != nil{
		return err
	}
    //将src的访问权限设置给dst
	if srcInfo, err = os.Stat(src); err != nil{
		return err
	}

	return os.Chmod(dst, srcInfo.Mode())
}