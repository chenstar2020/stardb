package cmd

import (
	"errors"
	"fmt"
	"github.com/tidwall/redcon"
)

var ErrSyntaxIncorrect = errors.New("syntax err")

var okResult = redcon.SimpleString("OK")

func newWrongNumOfArgsError(cmd string) error{
	return fmt.Errorf("wrong number of arguments for '%s' command", cmd)
}