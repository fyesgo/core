package util

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/user"
	"runtime"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	log "github.com/noxiouz/zapctx/ctxlog"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	FormattedBigIntLength = 80
)

func GetUserHomeDir() (homeDir string, err error) {
	usr, err := user.Current()
	if err != nil {
		return "", err
	}
	return usr.HomeDir, nil
}

func ParseEndpointPort(s string) (string, error) {
	_, port, err := net.SplitHostPort(s)
	if err != nil {
		return "", err
	}

	intPort, err := strconv.Atoi(port)
	if err != nil {
		return "", err
	}

	if intPort < 1 || intPort > 65535 {
		return "", errors.New("invalid port value")
	}

	return port, nil
}

func GetPlatformName() string {
	return fmt.Sprintf("%s/%s/%s", runtime.GOOS, runtime.GOARCH, runtime.Version())
}

func PubKeyToAddr(key ecdsa.PublicKey) common.Address {
	return crypto.PubkeyToAddress(key)
}

func FileExists(p string) bool {
	f, err := os.Stat(p)
	if err != nil {
		return !os.IsNotExist(err) && !f.IsDir()
	}

	return !f.IsDir()
}

// DirectoryExists returns true if the given directory exists
func DirectoryExists(p string) bool {
	f, err := os.Stat(p)
	if err != nil {
		return false
	}

	return f.IsDir()
}

// ParseBigInt parses the given string and converts it to *big.Int
func ParseBigInt(s string) (*big.Int, error) {
	n := new(big.Int)
	n, ok := n.SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("cannot convert %s to big.Int", s)
	}

	return n, nil
}

// StringToEtherPrice converts input string s to Ethereum's price present as big.Int
// This function expects to receive trimmed input.
func StringToEtherPrice(s string) (*big.Int, error) {
	bigFloat, ok := big.NewFloat(0).SetString(s)
	if !ok {
		return nil, fmt.Errorf("cannot convert %s to float value", s)
	}

	if bigFloat.Cmp(big.NewFloat(0)) < 0 {
		return nil, errors.New("value cannot be negative")
	}

	v, _ := big.NewFloat(0).Mul(bigFloat, big.NewFloat(params.Ether)).Int(nil)

	if v.Cmp(big.NewInt(0)) == 0 && bigFloat.Cmp(big.NewFloat(0)) > 0 {
		return nil, errors.New("value is too low")
	}

	return v, nil
}

func GetAvailableIPs() (availableIPs []net.IP, err error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for _, i := range ifaces {
		addrs, err := i.Addrs()
		if err != nil {
			return nil, err
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip != nil && ip.IsGlobalUnicast() {
				availableIPs = append(availableIPs, ip)
			}
		}
	}
	if len(availableIPs) == 0 {
		return nil, errors.New("could not determine a single unicast addr, check networking")
	}

	return availableIPs, nil
}

func IsPrivateIP(ip net.IP) bool {
	return isPrivateIPv4(ip) || isPrivateIPv6(ip)
}

func isPrivateIPv4(ip net.IP) bool {
	_, private24BitBlock, _ := net.ParseCIDR("10.0.0.0/8")
	_, private20BitBlock, _ := net.ParseCIDR("172.16.0.0/12")
	_, private16BitBlock, _ := net.ParseCIDR("192.168.0.0/16")
	return private24BitBlock.Contains(ip) ||
		private20BitBlock.Contains(ip) ||
		private16BitBlock.Contains(ip) ||
		ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast()
}

func isPrivateIPv6(ip net.IP) bool {
	_, block, _ := net.ParseCIDR("fc00::/7")

	return block.Contains(ip) || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast()
}

func StartPrometheus(ctx context.Context, listenAddr string) {
	log.GetLogger(ctx).Info(
		"starting metrics server", zap.String("metrics_addr", listenAddr))
	http.Handle("/metrics", promhttp.Handler())
	http.ListenAndServe(listenAddr, nil)
}

func BigIntToPaddedString(x *big.Int) string {
	raw := x.String()
	paddingSize := FormattedBigIntLength - len(raw)
	padding := make([]rune, paddingSize)
	for idx := range padding {
		padding[idx] = '0'
	}

	return string(padding) + raw
}

func HexToAddress(hex string) (common.Address, error) {
	if !common.IsHexAddress(hex) {
		return common.Address{}, fmt.Errorf("invalid address %s specified for parsing", hex)
	}
	return common.HexToAddress(hex), nil
}

func LaconicError(err error) zapcore.Field {
	return zap.String("error", err.Error())
}
