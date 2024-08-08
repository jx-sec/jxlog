package main

// {"bytes_received":529,"bytes_sent":2921,"connections_active":1,"connections_waiting":0,"content_length":29,"content_type":"application/x-www-form-urlencoded","cookie":"","host":"119.29.76.112","method":"POST","process_time":0.029,"query_string":"","raw_body":"username=admin&psd=Feefifofum","raw_headers":"connectionkeep-alive","user-agent": "Mozilla/5.0","accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8","content-type": "application/x-www-form-urlencoded","accept-encoding": "gzip, deflate","host": "119.29.76.112:80","origin": "http//119.29.76.112:80","referer": "http://119.29.76.112:80/admin/login.asp","upgrade-insecure-requests": 1,"content-length": 29,"accept-language": "en-GB,en;q=0.5","raw_resp_headersconnection":"close","raw_resp_headerscontent-encoding":"gzip","raw_resp_headerscontent-type":"text/html","raw_resp_headerstransfer-encoding":"chunked","referer":"","request_id":"ed9343f2dc16d77470a05ae9b1f04eeb","request_time":"2022-08-29 11:53:38","scheme":"http","src_ip":"64.227.104.242","ssl_ciphers":"","ssl_protocol":"","status":404,"upstream_addr":"119.29.76.112:8000","upstream_bytes_received":37708,"upstream_bytes_sent":584,"upstream_response_time":0.029,"upstream_status":404,"uri":"/boaform/admin/formLogin","user_agent":"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:71.0) Gecko/20100101 Firefox/71.0","version":"1.1","waf_action":"add_shared_dict_key","waf_extra":"scan111","waf_module":"web_rule_protection","waf_node_uuid":"c3c5f016-136c-4f7b-aed0-613480bd164d","waf_policy":"scan1","x_forwarded_for":""}
import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"bytes"
	"log"
	"net"
	"os"
	"time"
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/mitchellh/mapstructure"
)

func getEnv(key, fallback string) string {
    if value, ok := os.LookupEnv(key); ok {
        return value
    }
    return fallback
}

var (
	Clickhouse = getEnv("CLICKHOUSE","127.0.0.1:9000")
	Database = getEnv("Database","jxwaf")
	Username = getEnv("USERNAME","jxlog")
	Password = getEnv("PASSWORD","jxlog")
	Table = getEnv("TABLE","jxlog")

	TcpServer  = getEnv("TCPSERVER","0.0.0.0")
	TcpPort  = getEnv("TCPPORT","8877")

	// Clickhouse = "127.0.0.1:9000"
	// Database   = "my_database"
	// Username   = "username"
	// Password   = "password"
	// Table      = "jxlog"

	// TcpServer  = "127.0.0.1"
	// TcpPort  = "80"

)

type JxLog struct {
	Host                           string  `json:"host,string" mapstructure:"host"`
	RequestUuid                    string  `json:"request_uuid,string" mapstructure:"request_uuid"`
	WafNodeUUID                    string  `json:"waf_node_uuid,string" mapstructure:"waf_node_uuid"`
	UpstreamAddr                   string  `json:"upstream_addr,string" mapstructure:"upstream_addr"`
	UpstreamResponseTime           string  `json:"upstream_response_time,string" mapstructure:"upstream_response_time"`
	UpstreamStatus                 string  `json:"upstream_status,string" mapstructure:"upstream_status"`
	Status                         string  `json:"status,string" mapstructure:"status"`
	ProcessTime                    string  `json:"process_time,string" mapstructure:"process_time"`
	RequestTime                    string  `json:"request_time,string" mapstructure:"request_time"`
	RawHeaders                     string  `json:"raw_headers,string" mapstructure:"raw_headers"`
	Scheme                         string  `json:"scheme,string" mapstructure:"scheme"`
	Version                        string  `json:"version,string" mapstructure:"version"`
	URI                            string  `json:"uri,string" mapstructure:"uri"`
	RequestUri                     string  `json:"request_uri,string" mapstructure:"request_uri"`
	Method                         string  `json:"method,string" mapstructure:"method"`
	QueryString                    string  `json:"query_string,string" mapstructure:"query_string"`
	RawBody                        string  `json:"raw_body,string" mapstructure:"raw_body"`
	SrcIP                          string  `json:"src_ip,string" mapstructure:"src_ip"`
	UserAgent                      string  `json:"user_agent,string" mapstructure:"user_agent"`
	Cookie                         string  `json:"cookie,string" mapstructure:"cookie"`
	RawRespHeaders                 string  `json:"raw_resp_headers,string" mapstructure:"raw_resp_headers"`
	RawRespBody                    string  `json:"raw_resp_body,string" mapstructure:"raw_resp_body"`
	IsoCode                        string  `json:"iso_code,string" mapstructure:"iso_code"`
	City                           string  `json:"city,string" mapstructure:"city"`
	WafModule                      string  `json:"waf_module,string" mapstructure:"waf_module"`
	WafPolicy                      string  `json:"waf_policy,string" mapstructure:"waf_policy"`
	WafAction                      string  `json:"waf_action,string" mapstructure:"waf_action"`
	WafExtra                       string  `json:"waf_extra,string" mapstructure:"waf_extra"`
	JxwafDevid                     string  `json:"jxwaf_devid,string" mapstructure:"jxwaf_devid"`
}

type TcpCon struct {
}

type ClickHouse struct {
	conn clickhouse.Conn
}


func Clickhouse_conn(Clickhouse string, Database string, Username string, Password string) *ClickHouse {
ddl := `
CREATE TABLE  IF NOT EXISTS ` + Table + `   (
	Host String,
	RequestUuid String,
	WafNodeUUID String,
	UpstreamAddr String,
	UpstreamResponseTime String,
	UpstreamStatus String,
	Status String,
	ProcessTime String,
	RequestTime String,
	RawHeaders String,
	Scheme String,
	Version String,
	URI String,
        RequestUri String,
	Method String,
	QueryString String,
	RawBody String,
	SrcIP String,
	UserAgent String,
	Cookie String,
	RawRespHeaders String,
	RawRespBody String,
        IsoCode  String,
	City  String,
	WafModule String,
	WafPolicy String,
	WafAction String,
	WafExtra String,
        JxwafDevid String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(toDateTime64(RequestTime,0))
ORDER BY (toDateTime64(RequestTime,0))
TTL toDateTime(RequestTime) + INTERVAL 180 DAY
`
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{Clickhouse},
			Auth: clickhouse.Auth{
				Database: Database,
				Username: Username,
				Password: Password,
			},
			//Debug:           true,
			DialTimeout:     5 * time.Second,
			MaxOpenConns:    10,
			MaxIdleConns:    5,
			ConnMaxLifetime: time.Hour,
		})
	)
	if err != nil {
		log.Fatal(err)
	}
	// 调试table
	// if err := conn.Exec(ctx, "DROP TABLE IF EXISTS " + Table); err != nil {
	// 	log.Fatal(err)
	// }
	if err := conn.Exec(ctx, ddl); err != nil {
		log.Fatal(err)
	}
	// if err := example(conn); err != nil {
	// 	log.Fatal(err)
	// }
	c := new(ClickHouse)
	c.conn = conn
	return c
}

func JxlogHandle(data []byte)  JxLog{
	// var jxlog JxLog_raw
	var datamap map[string]interface{}
	err := json.Unmarshal(data, &datamap)
	if err != nil {
		log.Print("jxlog unmarshal err : ", err)
	}
	var jxlog JxLog
	config := &mapstructure.DecoderConfig{
		WeaklyTypedInput: true,
		Result:           &jxlog,
	}
	decoder, err := mapstructure.NewDecoder(config)
	if err != nil {
		log.Print("decode config err :",err)
	}
	// log.Print("datamap: ",datamap)
	err = decoder.Decode(datamap)
	if err != nil {
		log.Print("decode err : ",err)
	}
	
	return jxlog
}

func (c ClickHouse) Sendclickhous(data []byte) error {
	j := JxlogHandle(data)
	conn := c.conn
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+Table)
	if err != nil {
		return err
	}
	err = batch.AppendStruct(&j)
	if err != nil {
		return err
	}
	return batch.Send()
}

// func handleConn(con net.Conn,click *ClickHouse){
//     buf := make([]byte, 4096)
//     var jsonBuf bytes.Buffer
//     for {
//         n, err := con.Read(buf)

//         if n > 0 {
//             if buf[n-1] == 10 { // 10就是\n的ASCII
//                 // jsonBuf.Write(buf[:n-1]) // 去掉最后的换行符
// 				fmt.Println(jsonBuf.String())
//                 if err := click.Sendclickhous(jsonBuf.Bytes()); err != nil {
// 					log.Print("send clickhouse err : ", err)
// 				}
//                 jsonBuf.Reset() // 重置后用于下一次解析
//             } else {
//                 jsonBuf.Write(buf[:n])
//             }
//         }

//         if err != nil {
//             break
//         }
//     }
    
// }
func contentConn(jsonBuf []byte,click *ClickHouse) {
	// for{
		// log.Println("biof  errr",string(jsonBuf))
		if err := click.Sendclickhous(jsonBuf); err != nil {
			log.Print("send clickhouse err : ", err)
		}
	// }
}
	

func handleConn(con net.Conn, click *ClickHouse) {
	defer con.Close() // 确保连接被关闭
	reader := bufio.NewReader(con)
	var jsonBuf bytes.Buffer // 使用 bytes.Buffer 替代 []byte 以更高效地处理字符串

	for {
		line, isPrefix, err := reader.ReadLine()
		if len(line) > 0 {
			jsonBuf.Write(line)
			if !isPrefix {
				// 当一行被完全读取，处理缓冲区中的数据
				contentConn(jsonBuf.Bytes(), click)
				jsonBuf.Reset() // 重置缓冲区
			}
		}
		if err != nil {
			if err != io.EOF {
				log.Printf("Error reading from connection: %v", err)
			}
			return // 终止循环并关闭连接
		}
	}
}

func (ter TcpCon) Start() {
	click := Clickhouse_conn(Clickhouse, Database, Username, Password)
	listener, err := net.Listen("tcp", TcpServer+":"+TcpPort)
	if err != nil {
		log.Fatalf("Failed to start TCP listener: %v", err)
	}
	defer listener.Close() // 确保监听器被关闭

	log.Printf("TCP server started on %s:%s", TcpServer, TcpPort)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				log.Printf("Temporary error accepting connection: %v", netErr)
				continue
			}
			log.Fatalf("Error accepting connection: %v", err)
		}
		go handleConn(conn, click) // 使用协程处理新的连接
	}
}

func main() {

	t := TcpCon{}
	t.Start()
	
	
}
