// stdafx.h : include file for standard system include files,
//  or project specific include files that are used frequently, but
//      are changed infrequently
//

#if !defined(AFX_STDAFX_H__EE061F45_98C0_11D6_96CB_000347A347FE__INCLUDED_)
#define AFX_STDAFX_H__EE061F45_98C0_11D6_96CB_000347A347FE__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000
#define WIN32_LEAN_AND_MEAN		// Exclude rarely-used stuff from Windows headers
#define _CRT_SECURE_NO_DEPRECATE

#ifndef WINVER				// Allow use of features specific to Windows 95 and Windows NT 4 or later.
#define WINVER 0x0600		// Change this to the appropriate value to target Windows 98 and Windows 2000 or later.
#endif

#define VC_EXTRALEAN
#define _WIN32_DCOM

//#define EMBEDDED_VERSION

// TODO: reference additional headers your program requires here
#include <afx.h>
#include <winsvc.h>
#include <windows.h>
#include <process.h> 
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <atlbase.h>
#include <comdef.h>
#include <afxsock.h>		// MFC socket extensions
#include <afxtempl.h>
#include <afxmt.h>
#include <afxext.h>         // MFC extensions
#include <afxdisp.h>        // MFC OLE automation classes
#ifndef _AFX_NO_AFXCMN_SUPPORT
#include <afxcmn.h>			// MFC support for Windows Common Controls
#endif

#include <vector>
#include <map>
#include <algorithm>
#include <winhttp.h>
using namespace std;

#include "mosquittopp.h"
#include "ReadWriteLock.h"
#include "json/json.h"
#include "amqp/RabbitMQ.h"
#include "librdkafka/rdkafka.h"

typedef  unsigned  int    u_int32_t;
typedef  unsigned  short  u_int16_t;
typedef  unsigned  char   u_int8_t;
typedef  unsigned __int64 u_int64_t;
typedef unsigned char	byte_t;

#define    OPC_QUALITY_BAD             0x00
#define    OPC_QUALITY_UNCERTAIN       0x40
#define    OPC_QUALITY_GOOD            0xC0

#define BUFFER_SIZE			1024*64
#define FD_MAX(a,b)		((a) > (b) ? (a) : (b))

typedef struct buffer_
{
	char				buffer[BUFFER_SIZE];
	int					head;
}	buffer_t;

#define  NET_BIN_MAGIC       0x9834567
#define  NET_DAT_MAGIC       0x9434567
#define  NET_CMD_MAGIC       0x9234567
#define  NET_RES_MAGIC       0x9034567

#define  NET_RES_OK          1   //操作成功
#define  NET_RES_EEXIST      2   //项目已存在
#define  NET_RES_NOENT       3   //无此项目
#define  NET_RES_ERROR       4   //其它错误
#define  NET_RES_NODATA      5   //数据传输结束
#define  NET_RES_DENY        6
#define  NET_RES_WAIT		 7   //等待重发

#define  TNL_CMD_BASE		 200
#define  TNL_CMD_REGSRV			TNL_CMD_BASE + 1		//注册OPC服务到平台服务器
#define  TNL_CMD_REGVAL			TNL_CMD_BASE + 2		//注册OPC变量到平台服务器
#define  TNL_CMD_UPDATE			TNL_CMD_BASE + 3		//更新平台服务器上的OPC变量
#define  TNL_CMD_WRITE			TNL_CMD_BASE + 4		//由平台服务器写入OPC服务变量
#define  TNL_CMD_READ			TNL_CMD_BASE + 5		//由平台服务器即抄OPC变量信息
#define  TNL_CMD_HEAT			TNL_CMD_BASE + 6		//OPC隧道服务心跳
#define  TNL_CMD_GET_CONFIG		TNL_CMD_BASE + 36		//由平台服务器回读配置变量信息

#define  METER_CMD_BASE		 300
#define  METER_CMD_VALUE		METER_CMD_BASE + 1		//监控变量数据
#define  METER_CMD_SERIL		METER_CMD_BASE + 2		//监控串口通讯

struct CmdHdr
{
    u_int32_t  uMagic;
    u_int32_t  uLen;
    u_int32_t  uCmd;
};

struct ResHdr
{
    u_int32_t  uMagic;
    u_int32_t  uLen;
    u_int32_t  uRes;
};


struct MapValueItem
{
	DWORD	TunnelID;
	DWORD	ValueID;
	char	szNodePath[1024];
	int		nUpFlag;
	int		nDataType;
	double	fDataValue;
	FILETIME nDataTime;
};


struct TunnelServerItem
{
	int			bSubFlag;
	char		szTunnelName[256];		//上行接口描述
	char		szMachineNO[128];		//路由器序列号
	char		szTunnelInfo[128];		//下行接口描述
	u_int32_t	TunnelID;
};

struct TunnelValueItem
{
	DWORD		id;
	char		no[32];						//唯一站号
	char		szOPCPath[1024];
	double		dblValue;
	int			ValueType;
	FILETIME	TimeStamp;
	u_int32_t	TunnelID;
};

struct MqttToOPCValueItem
{
	DWORD			id;
	char			no[32];					//唯一站号
	char			name[256];					//设备名称
	char			szSrcTopic[1024];
	char			szDstOPCPath[1024];
	double			fDiv;					//精度系数
	double			fDlt;					//数据偏差
	double			fDead;					//采样死区
	char			szUnit[16];				//变量单位
	char			szDispType[16];			//变量类型：布尔型、整型、浮点型
	int				nDispDiv;				//小数点位数
	FILETIME		dataTime;
	double			dblValue;
	DWORD			quality; 
	bool			bMonitorValue;
	int				bUpdateFlag;
};

struct MqttTunnelItem
{
	u_int32_t	TunnelID;
	char		szName[256];
	char		szType[32];
	char		szDirect[16];
	char		szCode[16];
	CString		szReadParseCode;
	CString		szWriteParseCode;

	char		szMasteHost[128];
	int			nMastePort;
	char		szSlaveHost[128];
	int			nSlavePort;
	char		szMqttHost[128];			//上行MQTT地址
	int			nMqttPort;					//上行MQTT端口
	char		szMqttFormat[16];			//上行MQTT数据格式
	char		szMqttUserName[32];
	char		szMqttPassWord[32];
	char		szMqttFilter[256];			//上行MQTT订阅主题

	char		szAmqpHost[128];			//上行AMQP地址
	int			nAmqpPort;					//上行AMQP端口
	char		szAmqpUserName[32];
	char		szAmqpPassWord[32];
	char		szAmqpExchange[256];
	char		szAmqpQueue[256];
	char		szAmqpFormat[16];

	char		szKafkaBroker[512];
	char		szKafkaUserName[32];
	char		szKafkaPassWord[32];
	char		szProducerTopic[256];
	char		szConsumerTopic[256];
	char		szKafkaFormat[16];

	char		szMqttSlaveHost[128];		//下行MQTT地址
	int			nMqttSlavePort;				//下行MQTT端口
	char		szMqttSlaveFormat[16];		//下行MQTT数据格式
	char		szMqttSlaveCode[16];
	char		szMqttSlaveUserName[32];
	char		szMqttSlavePassWord[32];
	char		szMqttSlaveFilter[256];		//下行MQTT订阅主题
	DWORD		dataTime;
	int			bBackStatus;
	int			bUpStatus;
	int			bDownStatus;
	int			nActiveTime;
	vector<struct MqttToOPCValueItem> list;
	//
	HANDLE			hReadOPCMutex;
	CString		szMqttReadParseCode;
	CString		szMqttWriteParseCode;
};

struct ProcessParamItem
{
	HANDLE hThread;
	HANDLE hProcess;			//子进程句柄
	char				szTunnelName[256];
	DWORD				nTunnelID;
};


struct ProcessStatusItem
{
	char			szType[32];
	DWORD		nTunnelID;
	INT			nStatus;		//0——Add，1——Mdy，2——Delete
};

#define  MIX_CMD_BASE		 500
#define  MIX_CMD_HEAT		MIX_CMD_BASE + 1		//IoT服务心跳
#define  MIX_CMD_STOP		MIX_CMD_BASE + 2		//IoT服务停止

typedef struct _IoTServerStatus
{
	TCHAR	szIoTName[256];		//IoT服务名称
	DWORD	nProcessID;			//服务进程ID
	DWORD	nRunTime;			//服务运行时间
	INT		nRunStat;			//服务运行状态
}IoTServerStatus, *PIoTServerStatus;

int SendSocketData(SOCKET s, char *DataSendBuff,int len);
int ReadSocketData(SOCKET s, char *DataRcvBuff, int len);
int SendSocketCMD(SOCKET s, u_int32_t CmdMsg, u_int32_t len);
int ReadSocketCMD(SOCKET s, u_int32_t  *CmdMsg,u_int32_t  *uLen);
int SendSocketRES(SOCKET s, u_int32_t uRes, u_int32_t len);
int ReadSocketRES(SOCKET s, u_int32_t  *uRes,u_int32_t  *uLen);

CString ConvertGBKToUtf16(CString strGBK);
CString ConvertGBKToUtf8(CString strGBK);
CString ConvertUtf8ToGBK(CString strUTF8);
CString ConvertUtf16ToGBK(CStringW strUTF16);

CString FormatValueType(double fValue, const char *szType, int nDispDiv);

void WriteCSVFile(DWORD TunnelID, BOOL bFirst, const char* fmt, ...);
//查找字符串
int  FindingString(const char* lpszSour, const char* lpszFind, int nStart = 0);
//带通配符的字符串匹配
bool MatchingString(const char* lpszSour, const char* lpszMatch, bool bMatchCase = true);
//多重匹配
bool MultiMatching(const char* lpszSour, const char* lpszMatch, int nMatchLogic = 0, bool bRetReversed = 0, bool bMatchCase = true);

void LogEvent(LPCSTR pszFormat, ...);

//{{AFX_INSERT_LOCATION}}
// Microsoft Visual C++ will insert additional declarations immediately before the previous line.
const int PRODUCER_INIT_FAILED = -1;
const int PRODUCER_INIT_SUCCESS = 0;
const int PUSH_DATA_FAILED = -1;
const int PUSH_DATA_SUCCESS = 0;


static void dr_msg_cb(rd_kafka_t *rk,
	const rd_kafka_message_t *rkmessage, void *opaque) {
	if (rkmessage->err)
		LogEvent("%% Message delivery failed: %s\n",
			rd_kafka_err2str(rkmessage->err));
}

static void logger(const rd_kafka_t *rk, int level, const char *fac, const char *buf)
{
}

class ProducerKafka : public  CObject
{
public:
	ProducerKafka();
	~ProducerKafka();
	//
	struct MqttTunnelItem *pMqttTunnelItem;

	PVOID		m_pMqttHandle;
	int			m_bConnectFlag;

	int init_kafka(int partition, char *brokers, char *username, char *password, char *topic);
	int push_data_to_kafka(const char* buf, const int buf_len, const char* key, const int key_len);
	void destroy();

private:
	int partition_;

	//rd    
	rd_kafka_t* handler_;
	rd_kafka_conf_t *conf_;

	//topic    
	rd_kafka_topic_t *topic_;
};


static int run = 1;
//`rd_kafka_t`自带一个可选的配置API，如果没有调用API，Librdkafka将会使用CONFIGURATION.md中的默认配置。  
static rd_kafka_t *rk;
static rd_kafka_topic_partition_list_t *topics;

class ConsummerKafka : public  CObject
{
public:
	ConsummerKafka();
	~ConsummerKafka();

	struct MqttTunnelItem *pMqttTunnelItem;

	CMap<CString, LPCSTR, DOUBLE, DOUBLE>	 m_MapTopicValue;
	CMap<CString, LPCSTR, CString, CString>	 m_MapPayload;
	CMap<CString, LPCSTR, CString, CString>	 m_MapTopicSrc;
	CMap<CString, LPCSTR, CString, CString>	 m_MapTopicDst;

	CMap<CString, LPCSTR, struct MqttToOPCValueItem*, struct MqttToOPCValueItem*>	m_MqttMapTable1;
	CMap<CString, LPCSTR, struct MqttToOPCValueItem*, struct MqttToOPCValueItem*>	m_MqttMapTable2;
	//
	PVOID		m_pMqttHandle;
	int			m_bConnectFlag;

	int init_kafka(char *brokers, char *username, char *password, char *group, char *topic);
	int pull_data_from_kafka();
	int msg_consume(rd_kafka_message_t *rkmessage, void *opaque);
	void destroy();

private:
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	rd_kafka_resp_err_t err;
};

class CAmqpSmart : public  CRabbitMQ
{
public:
	CAmqpSmart(string HostName = "localhost", uint32_t port = 5672, string usr = "guest", string psw = "guest");
	~CAmqpSmart();
	//
	struct MqttTunnelItem *pMqttTunnelItem;

	CMap<CString, LPCSTR, DOUBLE, DOUBLE>	 m_MapTopicValue;
	CMap<CString, LPCSTR, CString, CString>	 m_MapPayload;
	CMap<CString, LPCSTR, CString, CString>	 m_MapTopicSrc;
	CMap<CString, LPCSTR, CString, CString>	 m_MapTopicDst;

	CMap<CString, LPCSTR, struct MqttToOPCValueItem*, struct MqttToOPCValueItem*>	m_MqttMapTable1;
	CMap<CString, LPCSTR, struct MqttToOPCValueItem*, struct MqttToOPCValueItem*>	m_MqttMapTable2;
	//
	PVOID		m_pMqttHandle;
	int			m_bConnectFlag;

	int	subscribe();
};


class CMqttSmart : public mosqpp::mosquittopp
{
public:
	CMqttSmart(const char *id, const char *host, int port, const char *username, const char *password);
	~CMqttSmart();
	//
	struct MqttTunnelItem *pMqttTunnelItem;

	CMap<CString, LPCSTR, DOUBLE, DOUBLE>	 m_MapTopicValue;
	CMap<CString, LPCSTR, CString, CString>	 m_MapPayload;
	CMap<CString, LPCSTR, CString, CString>	 m_MapTopicSrc;
	CMap<CString, LPCSTR, CString, CString>	 m_MapTopicDst;

	CMap<CString, LPCSTR, struct MqttToOPCValueItem*, struct MqttToOPCValueItem*>	m_MqttMapTable1;
	CMap<CString, LPCSTR, struct MqttToOPCValueItem*, struct MqttToOPCValueItem*>	m_MqttMapTable2;
	//
	PVOID		m_pMqttHandle;
	int			m_bConnectFlag;
	//
	void on_connect(int rc);
	void on_message(const struct mosquitto_message *message);
	void on_subscribe(int mid, int qos_count, const int *granted_qos);
};

class CMqttTunnel : public mosqpp::mosquittopp
{
public:
	CMqttTunnel(const char *id, const char *host, int port, const char *username, const char *password, const char *topic);
	~CMqttTunnel();

	char m_szMqttID[64];
	char m_szTopic[64];
	struct MqttTunnelItem *pMqttTunnelItem; 

	CMap<CString, LPCSTR, DOUBLE, DOUBLE>	 m_MapTopicValue;
	CMap<CString, LPCSTR, CString, CString>	 m_MapPayload;
	CMap<CString, LPCSTR, CString, CString>	 m_MapTopicSrc;
	CMap<CString, LPCSTR, CString, CString>	 m_MapTopicDst;

	CMap<CString, LPCSTR, struct MqttToOPCValueItem*, struct MqttToOPCValueItem*>	m_MqttMapTable1;
	CMap<CString, LPCSTR, struct MqttToOPCValueItem*, struct MqttToOPCValueItem*>	m_MqttMapTable2;
	CMap<DWORD, DWORD, struct MqttToOPCValueItem*, struct MqttToOPCValueItem*>		m_MqttMapTable3;
	CMap<CString, LPCSTR, struct MqttToOPCValueItem*, struct MqttToOPCValueItem*>	m_MqttMapTable4;
	//
	PVOID			m_pMqttHandle;
	PVOID			m_pAmqpHandle; 
	PVOID			m_pConsummerHandle;
	PVOID			m_pKafkaHandle; 
	//
	SOCKET			m_ServerSock;
	CTime			m_RefreshTime;
	HANDLE			hSockMutex;
	//
	SOCKET			m_SubSock;
	int				nSubPort;
	CString			m_szIPAddr;
	int				bSubFlag;
	int				bHisFlag;
	int				nHisCount;
	CMap<DWORD, DWORD, FILETIME, FILETIME> m_HisTimeStampMap;
	//
	int RegServer(void);
	int HeatServer(void);
	int UpdateList(char *pReadBuffer, int nCount);
	void SaveList(char *pReadBuffer, int nCount);
	void SaveItem(struct TunnelValueItem *pValue);
	void PlayItem(void);
	void SaveLast(void);
	void PlayLast(void);
	int UpdateItem(struct TunnelValueItem *pValue);
	int WriteItem(struct TunnelValueItem *pValue); 
	int ReadItem(const char *path); 
	void ReadCSVFile(DWORD TunnelID);
	//
	void on_connect(int rc);
	void on_message(const struct mosquitto_message *message);
	void on_subscribe(int mid, int qos_count, const int *granted_qos);
	//
	SOCKET			sMeter;
	void SendDataToMeter(struct MapValueItem *pValue);
	HINTERNET  hSession, hConnect;
	DWORD m_ParseTryCount;
	int ParseMqttPayload(const char* lpszParseCode, const char* lpszTopic, const char* lpszPayloadType, const char* lpszPayload, int nPayloadLen, CString &lpszParseResult);
};

//
#if _MSC_VER >= 1100
template<> UINT AFXAPI HashKey<LPCSTR> (LPCSTR key);
#else
UINT AFXAPI HashKey(LPCSTR key);
#endif

#endif // !defined(AFX_STDAFX_H__EE061F45_98C0_11D6_96CB_000347A347FE__INCLUDED_)
