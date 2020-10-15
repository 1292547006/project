// Service.cpp: implementation of the CService class.
//thingslabs  1234
//////////////////////////////////////////////////////////////////////

#include "stdafx.h"
#include <stdio.h>
#include <psapi.h>
#include "Service.h"
#include "IniFile.h"


CService _Module;
//
volatile	int	g_RunServrice=-1;
volatile	int	g_DebugMode=1;
volatile	int	g_OffineSpan=60;
volatile	int	g_OffineRecord=10000;
volatile	int	g_ActiveTime=900;
volatile	int g_SynThreadCount=0;
CString		g_szMachineNO, g_szApplyDesc;
char		g_ServricePath[8192];
char		g_DebugLogPath[8192];
vector<CMqttTunnel*>	g_MqttTunnelList;
//
HANDLE g_hMqttWriteMutex;
BOOL g_bMqttConnect = FALSE;
mosqpp::mosquittopp		*g_pMqttPub;

SOCKET  sMonit = INVALID_SOCKET;
void SendStatusToMonit(IoTServerStatus *pValue)
{
	if (sMonit == INVALID_SOCKET)
	{
		sMonit = socket(PF_INET, SOCK_DGRAM, 0);
		//
		if (sMonit == SOCKET_ERROR)
		{
			LogEvent("SendStatusToMonit:无法创建Socket服务！");
			return;
		}
		//
		int timeout = 500; //ms
		setsockopt(sMonit, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout));
	}

	SOCKADDR_IN			local;
	local.sin_addr.s_addr = inet_addr("127.0.0.1");
	local.sin_family = AF_INET;
	local.sin_port = htons(20000);
	//
	struct CmdHdr rp;
	rp.uCmd = MIX_CMD_HEAT;
	rp.uLen = sizeof(IoTServerStatus);
	rp.uMagic = NET_CMD_MAGIC;
	//
	if (sendto(sMonit, (char *)&rp, sizeof(struct CmdHdr), 0, (struct sockaddr*)&local, sizeof(local)) == -1)
	{
		LogEvent("SendStatusToMonit:发送监控命令错误！");
		closesocket(sMonit);
		sMonit = INVALID_SOCKET;
		return;
	}
	//
	if (sendto(sMonit, (char *)pValue, sizeof(IoTServerStatus), 0, (struct sockaddr*)&local, sizeof(local)) == -1)
	{
		LogEvent("SendStatusToMonit:发送监控数据错误！");
		closesocket(sMonit);
		sMonit = INVALID_SOCKET;
		return;
	}
}

//
#define   SD_LOGOFF               EWX_LOGOFF   
#define   SD_SHUTDOWN           EWX_POWEROFF   
#define   SD_REBOOT               EWX_REBOOT   
#define   SD_SUSPEND             0x00000020           
#define   SD_MONITOR             0x00000040     
#define   SD_SCREENSAVE       0x00000080     //the   parameter   bForce   will   be   ignored

BOOL KillProcess(char *strProcessName)
{
	DWORD aProcesses[1024], cbNeeded, cbMNeeded;
	HMODULE hMods[1024];
	HANDLE hProcess;
	char szProcessName[MAX_PATH];
	if ( !EnumProcesses( aProcesses, sizeof(aProcesses), &cbNeeded ) ) return 0;
	for(int i=0; i< (int) (cbNeeded / sizeof(DWORD)); i++)
	{
		hProcess = OpenProcess( PROCESS_QUERY_INFORMATION | PROCESS_VM_READ | PROCESS_TERMINATE, FALSE, aProcesses[i]);
		if(hProcess)
		{
			if(EnumProcessModules(hProcess, hMods, sizeof(hMods), &cbMNeeded))
			{
				GetModuleFileNameEx( hProcess, hMods[0], szProcessName,sizeof(szProcessName));
				if(strstr(szProcessName, strProcessName))
				{
					TerminateProcess(hProcess, 0);
					CloseHandle(hProcess);
					return TRUE;
				}
			}
			//
			CloseHandle(hProcess);
		}
	}
	return FALSE;
}


#define MAX_PROCESSES	1024

HANDLE		FcpJobObject;
int			process_fp[MAX_PROCESSES];
int			process_idx = 0;

DWORD WINAPI spawn_process(LPVOID lpParam)
{
	struct ProcessParamItem *	pConfigItem = (struct ProcessParamItem *)lpParam;

	int idx = process_idx++, ret;
	char cCommandLine[MAX_PATH*4];
	DWORD dwSize = GetModuleFileName(NULL, cCommandLine, MAX_PATH * 4);
	cCommandLine[dwSize] = 0;
	// 启动命令行
	char sCommandLine[MAX_PATH * 4];
	sprintf(sCommandLine,"%s %s %d", cCommandLine, pConfigItem->szTunnelName, pConfigItem->nTunnelID);
	//
	while (g_RunServrice) {
		STARTUPINFO si = { 0 };
		PROCESS_INFORMATION pi = { 0 };
		ZeroMemory(&si, sizeof(STARTUPINFO));
		si.cb = sizeof(STARTUPINFO);
		si.dwFlags = STARTF_USESTDHANDLES;
		si.hStdInput = INVALID_HANDLE_VALUE;
		si.hStdOutput = INVALID_HANDLE_VALUE;
		si.hStdError = INVALID_HANDLE_VALUE;
		if (0 == (ret = CreateProcess(NULL, sCommandLine,
			NULL, NULL,
			TRUE, CREATE_NO_WINDOW | CREATE_SUSPENDED | CREATE_BREAKAWAY_FROM_JOB,
			NULL, NULL,
			&si, &pi))) {
			LogEvent("启动子进程 %s【%lu】失败", pConfigItem->szTunnelName, pConfigItem->nTunnelID);
			continue;
		}

		/* Use Job Control System */
		if (!AssignProcessToJobObject(FcpJobObject, pi.hProcess)) {
			LogEvent("加入子进程 %s【%lu】失败", pConfigItem->szTunnelName, pConfigItem->nTunnelID);
			DWORD dw = GetLastError();
			TerminateProcess(pi.hProcess, 1);
			CloseHandle(pi.hProcess);
			CloseHandle(pi.hThread);
			continue;
		}

		if (!ResumeThread(pi.hThread)) {
			LogEvent("运行子进程 %s【%lu】失败", pConfigItem->szTunnelName, pConfigItem->nTunnelID);
			TerminateProcess(pi.hProcess, 1);
			CloseHandle(pi.hProcess);
			CloseHandle(pi.hThread);
			continue;
		}
		//
		LogEvent("启动子进程 %s【%lu】成功", pConfigItem->szTunnelName, pConfigItem->nTunnelID);
		pConfigItem->hProcess = pi.hProcess;
		pConfigItem->hThread = pi.hThread;
		//
		process_fp[idx] = (int)pi.hProcess;
		WaitForSingleObject(pi.hProcess, INFINITE);
		process_fp[idx] = 0;
		CloseHandle(pi.hProcess);
		CloseHandle(pi.hThread);

		LogEvent("退出子进程 %s【%lu】成功", pConfigItem->szTunnelName, pConfigItem->nTunnelID);
		Sleep(5000);
	}
	g_SynThreadCount--;
	return 0;
}

DWORD WINAPI SubTunnelThread(LPVOID lpParam)
{
	CMqttTunnel *pMqttDef=(CMqttTunnel	*)lpParam;
	pMqttDef->bSubFlag=1;
	//
	SOCKADDR_IN		m_SockStruct;
	CTime			m_HeatTime=CTime::GetCurrentTime();
	int				rets=0;
	u_int32_t		uLen;
	u_int32_t		uCmd;
	u_int32_t		uRes;
	WSADATA			wsaData;
	if (WSAStartup(MAKEWORD(2,1),&wsaData)) 
	{
		LogEvent("Winsock无法初始化！");		
		pMqttDef->bSubFlag=0;
		g_SynThreadCount--;
		return -1;
	}
	//
	pMqttDef->m_SubSock = INVALID_SOCKET;
	m_SockStruct.sin_family=AF_INET;                  //使用TCP/IP协议
	m_SockStruct.sin_port = htons(pMqttDef->nSubPort);
	m_SockStruct.sin_addr.S_un.S_addr=inet_addr(pMqttDef->m_szIPAddr);
	//
	while(g_RunServrice>0 && pMqttDef->m_ServerSock!=INVALID_SOCKET)
	{
		if(pMqttDef->m_SubSock == INVALID_SOCKET)
		{
			pMqttDef->m_SubSock=socket(PF_INET,SOCK_STREAM,0);
			if(pMqttDef->m_SubSock == INVALID_SOCKET)
			{
				LogEvent("无法创建OPC隧道客户端Socket！");
				Sleep(1000);
				continue;
			}	
			if(connect(pMqttDef->m_SubSock,(LPSOCKADDR)&m_SockStruct,sizeof(m_SockStruct)) == SOCKET_ERROR)
			{		
				LogEvent("无法连接到MQTT隧道服务端(%s:%d)！", pMqttDef->m_szIPAddr, pMqttDef->nSubPort);
				closesocket(pMqttDef->m_SubSock);
				pMqttDef->m_SubSock = INVALID_SOCKET;
				closesocket(pMqttDef->m_ServerSock);
				pMqttDef->m_ServerSock = INVALID_SOCKET;
				Sleep(1000);
				continue;
			}
			//
			if(SendSocketCMD(pMqttDef->m_SubSock,TNL_CMD_REGSRV,sizeof(struct TunnelServerItem)) == -1)
			{
				LogEvent("无法发送数据到MQTT隧道服务端13(%s:%d)！", pMqttDef->m_szIPAddr, pMqttDef->nSubPort);
				closesocket(pMqttDef->m_SubSock);
				pMqttDef->m_SubSock = INVALID_SOCKET;
				closesocket(pMqttDef->m_ServerSock);
				pMqttDef->m_ServerSock = INVALID_SOCKET;
				Sleep(1000);
				continue;
			}
			//
			struct TunnelServerItem info1;
			memset(&info1,0,sizeof(struct TunnelServerItem));
			sprintf_s(info1.szTunnelName,"%s", pMqttDef->pMqttTunnelItem->szName);
			sprintf_s(info1.szTunnelInfo, "%s:%d", pMqttDef->pMqttTunnelItem->szMqttSlaveHost, pMqttDef->pMqttTunnelItem->nMqttSlavePort);
			sprintf_s(info1.szMachineNO,"%s", g_szMachineNO);
			info1.TunnelID= pMqttDef->pMqttTunnelItem->TunnelID;
			info1.bSubFlag=1;
			//
			if(SendSocketData(pMqttDef->m_SubSock,(char *)&info1,sizeof(info1)) == -1)
			{
				LogEvent("无法发送数据到MQTT隧道服务端12(%s:%d)！", pMqttDef->m_szIPAddr, pMqttDef->nSubPort);
				closesocket(pMqttDef->m_SubSock);
				pMqttDef->m_SubSock = INVALID_SOCKET;
				closesocket(pMqttDef->m_ServerSock);
				pMqttDef->m_ServerSock = INVALID_SOCKET;
				Sleep(1000);
				continue;
			}
			rets=ReadSocketRES(pMqttDef->m_SubSock,&uRes,&uLen);
			if(rets<=0)
			{
				LogEvent("无法读取MQTT隧道服务端命令确认(%s:%d)！", pMqttDef->m_szIPAddr, pMqttDef->nSubPort);
				closesocket(pMqttDef->m_SubSock);
				pMqttDef->m_SubSock = INVALID_SOCKET;
				closesocket(pMqttDef->m_ServerSock);
				pMqttDef->m_ServerSock = INVALID_SOCKET;
				Sleep(1000);
				continue;
			}
			if(uRes!=NET_RES_OK)
			{
				LogEvent("向平台注册MQTT隧道服务失败(%s)！", pMqttDef->pMqttTunnelItem->szName);
				closesocket(pMqttDef->m_SubSock);
				pMqttDef->m_SubSock = INVALID_SOCKET;
				closesocket(pMqttDef->m_ServerSock);
				pMqttDef->m_ServerSock = INVALID_SOCKET;
				Sleep(1000);
				continue;
			}
		}
		//
		CTime tc = CTime::GetCurrentTime();
		CTimeSpan ts = tc-m_HeatTime;
		if(ts.GetTotalSeconds()>=60)
		{
			LogEvent("MQTT隧道子服务超时(%s)！", pMqttDef->pMqttTunnelItem->szName);
			closesocket(pMqttDef->m_SubSock);
			pMqttDef->m_SubSock = INVALID_SOCKET;
			closesocket(pMqttDef->m_ServerSock);
			pMqttDef->m_ServerSock = INVALID_SOCKET;
			break;
		}
		//
		rets=ReadSocketCMD(pMqttDef->m_SubSock,&uCmd,&uLen);
		//
		if(rets<=0)
		{
			Sleep(1000);
			continue;
		}
		m_HeatTime=tc;
		//
		switch(uCmd)
		{
		case TNL_CMD_HEAT:
			{
				if(SendSocketRES(pMqttDef->m_SubSock,NET_RES_OK,0) == -1)
				{
					LogEvent("无法发送数据到MQTT隧道服务端11(%s)！", pMqttDef->pMqttTunnelItem->szName);
					closesocket(pMqttDef->m_SubSock);
					pMqttDef->m_SubSock = INVALID_SOCKET;
					closesocket(pMqttDef->m_ServerSock);
					pMqttDef->m_ServerSock = INVALID_SOCKET;
					Sleep(1000);
					continue;
				}
			}
			break;
		case TNL_CMD_WRITE:
			{
				struct TunnelValueItem info1;
				memset(&info1,0,sizeof(struct TunnelValueItem));
				if(ReadSocketData(pMqttDef->m_SubSock,(char *)&info1,sizeof(struct TunnelValueItem)) == -1)
				{
					LogEvent("无法发送数据到MQTT隧道服务端10(%s)！", pMqttDef->pMqttTunnelItem->szName);
					closesocket(pMqttDef->m_SubSock);
					pMqttDef->m_SubSock = INVALID_SOCKET;
					closesocket(pMqttDef->m_ServerSock);
					pMqttDef->m_ServerSock = INVALID_SOCKET;
					Sleep(1000);
					continue;
				}
				//
				if(pMqttDef->WriteItem(&info1)==0)
				{
					if(SendSocketRES(pMqttDef->m_SubSock,NET_RES_OK,0) == -1)
					{
						LogEvent("无法发送数据到MQTT隧道服务端9(%s)！", pMqttDef->pMqttTunnelItem->szName);
						closesocket(pMqttDef->m_SubSock);
						pMqttDef->m_SubSock = INVALID_SOCKET;
						closesocket(pMqttDef->m_ServerSock);
						pMqttDef->m_ServerSock = INVALID_SOCKET;
						Sleep(1000);
						continue;
					}	
				}
				else
				{
					if(SendSocketRES(pMqttDef->m_SubSock,NET_RES_ERROR,0) == -1)
					{
						LogEvent("无法发送数据到MQTT隧道服务端8(%s)！", pMqttDef->pMqttTunnelItem->szName);
						closesocket(pMqttDef->m_SubSock);
						pMqttDef->m_SubSock = INVALID_SOCKET;
						closesocket(pMqttDef->m_ServerSock);
						pMqttDef->m_ServerSock = INVALID_SOCKET;
						Sleep(1000);
						continue;
					}	
				}
			}
			break;
		case TNL_CMD_READ:
			{
				char szPath[8192];
				memset(szPath,0,8192);
				if(uLen>0 && (uLen+1)<8192)
				{
					if(ReadSocketData(pMqttDef->m_SubSock,szPath,uLen) == -1)
					{
						LogEvent("无法发送数据到MQTT隧道服务端7(%s)！", pMqttDef->pMqttTunnelItem->szName);
						closesocket(pMqttDef->m_SubSock);
						pMqttDef->m_SubSock = INVALID_SOCKET;
						closesocket(pMqttDef->m_ServerSock);
						pMqttDef->m_ServerSock = INVALID_SOCKET;
						Sleep(1000);
						continue;
					}
					//
					if (SendSocketRES(pMqttDef->m_SubSock, NET_RES_OK, 0) == -1)
					{
						LogEvent("无法发送数据到MQTT隧道服务端9(%s)！", pMqttDef->pMqttTunnelItem->szName);
						closesocket(pMqttDef->m_SubSock);
						pMqttDef->m_SubSock = INVALID_SOCKET;
						closesocket(pMqttDef->m_ServerSock);
						pMqttDef->m_ServerSock = INVALID_SOCKET;
						Sleep(1000);
						continue;
					}
					//
					szPath[uLen] = '*';
					szPath[uLen+1] = '\0';
					if (pMqttDef->ReadItem(szPath) == -1)
					{
						LogEvent("获取MQTT隧道（%s）变量信息（%s）失败！", pMqttDef->pMqttTunnelItem->szName, szPath);
					}
				}
				else
				{
					if (SendSocketRES(pMqttDef->m_SubSock, NET_RES_ERROR, 0) == -1)
					{
						LogEvent("无法发送数据到MQTT隧道服务端8(%s)！", pMqttDef->pMqttTunnelItem->szName);
						closesocket(pMqttDef->m_SubSock);
						pMqttDef->m_SubSock = INVALID_SOCKET;
						closesocket(pMqttDef->m_ServerSock);
						pMqttDef->m_ServerSock = INVALID_SOCKET;
						Sleep(1000);
						continue;
					}
				}
			}
			break;
		default:
			break;
		}
	}
	//
	LogEvent("MQTT隧道子服务退出(%s)！", pMqttDef->pMqttTunnelItem->szName);
	//
	if(pMqttDef->m_SubSock!=INVALID_SOCKET)
	{
		closesocket(pMqttDef->m_SubSock);
		pMqttDef->m_SubSock = INVALID_SOCKET;
	}
	//
	if(pMqttDef->m_ServerSock!=INVALID_SOCKET)
	{
		closesocket(pMqttDef->m_ServerSock);
		pMqttDef->m_ServerSock = INVALID_SOCKET;
	}
	//
	pMqttDef->bSubFlag=0;
	g_SynThreadCount--;
	return 0;
}

DWORD WINAPI MasteTunnelThread(LPVOID lpParam)
{
	struct MqttTunnelItem	*pConfigItem=(struct MqttTunnelItem	*)lpParam;
	//
	TCHAR sMqttID[64];
	sprintf(sMqttID,"Master-%lu-MQTT", pConfigItem->TunnelID);
	CMqttTunnel *pMqttDef = new CMqttTunnel(sMqttID,pConfigItem->szMqttSlaveHost, pConfigItem->nMqttSlavePort, pConfigItem->szMqttSlaveUserName, pConfigItem->szMqttSlavePassWord, pConfigItem->szMqttSlaveFilter);
	if (pMqttDef == NULL)
	{
		g_SynThreadCount--;
		return 1;
	}
	pMqttDef->pMqttTunnelItem = pConfigItem;
	//
	pMqttDef->ReadCSVFile(pConfigItem->TunnelID);
	//
	for (DWORD i = 0; i < pConfigItem->list.size(); i++)
	{
		pMqttDef->m_MqttMapTable1.SetAt(pConfigItem->list[i].szSrcTopic, &(pConfigItem->list[i]));
		pMqttDef->m_MqttMapTable2.SetAt(pConfigItem->list[i].szDstOPCPath, &(pConfigItem->list[i]));
		pMqttDef->m_MqttMapTable3.SetAt(pConfigItem->list[i].id, &(pConfigItem->list[i]));
		CString szDstTopic = pConfigItem->list[i].szDstOPCPath;
		szDstTopic.Replace('.', '/');
		szDstTopic.Format("%s/%s", pConfigItem->list[i].no, szDstTopic.Mid(szDstTopic.ReverseFind('/') + 1));
		pMqttDef->m_MqttMapTable4.SetAt(szDstTopic, &(pConfigItem->list[i]));
	}
	//
	SOCKADDR_IN		m_SockStruct;
	int				rets=0;
	WSADATA			wsaData;
	if (WSAStartup(MAKEWORD(2,1),&wsaData)) 
	{
		LogEvent("Winsock无法初始化！");
		//
		delete pMqttDef;
		//
		g_SynThreadCount--;
		return -1;
	}
	//
	pMqttDef->m_ServerSock = INVALID_SOCKET;		
	m_SockStruct.sin_family=AF_INET;                  //使用TCP/IP协议
	m_SockStruct.sin_port = htons(pConfigItem->nMastePort);
	m_SockStruct.sin_addr.S_un.S_addr=inet_addr(pConfigItem->szMasteHost);
	pMqttDef->nSubPort = pConfigItem->nMastePort;
	pMqttDef->m_szIPAddr = pConfigItem->szMasteHost;
	g_MqttTunnelList.push_back(pMqttDef);
	//
	int nHeatCount = 0;
	while(g_RunServrice>0)
	{
		int rc = pMqttDef->loop();
		if (rc) {
			LogEvent("MasteTunnelThread #111 pMqttDef->loop rc=%d", rc);
			pMqttDef->reconnect();
			Sleep(1000);
		}
		//
		if(pMqttDef->m_ServerSock == INVALID_SOCKET)
		{
			if(pMqttDef->m_SubSock!=INVALID_SOCKET)
			{
				closesocket(pMqttDef->m_SubSock);
				pMqttDef->m_SubSock = INVALID_SOCKET;
			}
			//
			int nTryCount = 0;
			while (g_RunServrice>0 && pMqttDef->bSubFlag == 1)
			{
				LogEvent("pMqttDef->bSubFlag=%d！", pMqttDef->bSubFlag);
				Sleep(1000);
				nTryCount++;
				//
				if (nTryCount>10)
				{
					//g_RunServrice = 0;
					break;
				}
			}
			//
			pMqttDef->m_ServerSock=socket(PF_INET,SOCK_STREAM,0);  
			if(pMqttDef->m_ServerSock == INVALID_SOCKET)
			{
				LogEvent("无法创建MQTT隧道客户端Socket！");
				Sleep(1000);
				continue;
			}	
			if(connect(pMqttDef->m_ServerSock,(LPSOCKADDR)&m_SockStruct,sizeof(m_SockStruct)) == SOCKET_ERROR)
			{		
				LogEvent("无法连接到MQTT隧道服务端(%s:%d)！",pConfigItem->szMasteHost,pConfigItem->nMastePort);
				closesocket(pMqttDef->m_ServerSock);
				pMqttDef->m_ServerSock = INVALID_SOCKET;
				Sleep(1000);
				continue;
			}
			//准备重新注册MQTT隧道服务
			if(pMqttDef->RegServer()==-1)
			{
				LogEvent("注册MQTT隧道服务(%s)失败！",pConfigItem->szName);
				closesocket(pMqttDef->m_ServerSock);
				pMqttDef->m_ServerSock = INVALID_SOCKET;
				Sleep(1000*10);
				continue;
			}
			Sleep(1000);
			//
			HANDLE				hThread;
			DWORD				dwThread;
			hThread=CreateThread(NULL,0,SubTunnelThread,(LPVOID)pMqttDef,0,&dwThread);
			if(hThread==NULL)
			{
				LogEvent("创建MQTT隧道服务子线程异常!");
			}
			else
			{
				g_SynThreadCount++;
				CloseHandle(hThread);
			}
		}		
		//
		if (nHeatCount % 1000==0)
		{
			nHeatCount = 0;
			if (pMqttDef->HeatServer() == -1)
			{
				struct MapValueItem myValue;
				memset(&myValue, 0, sizeof(struct MapValueItem));
				myValue.TunnelID = pMqttDef->pMqttTunnelItem->TunnelID;
				myValue.ValueID = 0;
				myValue.nUpFlag = 0;
				myValue.nDataType = 0;
				myValue.fDataValue = (rc==0) ? 1 : 0;
				sprintf(myValue.szNodePath, "*");
				//
				pMqttDef->SendDataToMeter(&myValue);
				//
				LogEvent("MQTT隧道服务(%s)心跳检测失败！", pConfigItem->szName);
				closesocket(pMqttDef->m_ServerSock);
				pMqttDef->m_ServerSock = INVALID_SOCKET;
				Sleep(1000 * 10);
				continue;
			}
			else
			{
				if (rc == 0)
				{
					IoTServerStatus sStstaus;
					memset(&sStstaus, 0, sizeof(IoTServerStatus));
					strcpy(sStstaus.szIoTName, "MqttTunnel");
					sStstaus.nProcessID = GetCurrentProcessId();
					sStstaus.nRunStat = 1;
					//
					SendStatusToMonit(&sStstaus);
				}
				struct MapValueItem myValue;
				memset(&myValue, 0, sizeof(struct MapValueItem));
				myValue.TunnelID = pMqttDef->pMqttTunnelItem->TunnelID;
				myValue.ValueID = 0;
				myValue.nUpFlag = 1;
				myValue.nDataType = 0;
				myValue.fDataValue = (rc == 0) ? 1 : 0;
				sprintf(myValue.szNodePath, "*");
				//
				pMqttDef->SendDataToMeter(&myValue);
			}
		}
		else
		{
			nHeatCount++;
		}
	}
	//
	if(pMqttDef->m_SubSock!=INVALID_SOCKET)
	{
		closesocket(pMqttDef->m_SubSock);
		pMqttDef->m_SubSock = INVALID_SOCKET;
	}
	//
	if(pMqttDef->m_ServerSock!=INVALID_SOCKET)
	{
		closesocket(pMqttDef->m_ServerSock);
		pMqttDef->m_ServerSock = INVALID_SOCKET;
	}
	//
	int nTryCount = 0;
	while(pMqttDef->bSubFlag==1)
	{
		Sleep(1000);
		if (nTryCount>10)
		{
			//g_RunServrice = 0;
			break;
		}
	}
	//
	delete pMqttDef;
	//
	g_SynThreadCount--;
	return 0;
}

DWORD WINAPI SlaveTunnelThread(LPVOID lpParam)
{
	struct MqttTunnelItem	*pConfigItem = (struct MqttTunnelItem	*)lpParam;
	//
	TCHAR sMqttID[64];
	sprintf(sMqttID, "Slave-%lu-MQTT", pConfigItem->TunnelID);
	CMqttTunnel *pMqttDef = new CMqttTunnel(sMqttID, pConfigItem->szMqttSlaveHost, pConfigItem->nMqttSlavePort, pConfigItem->szMqttSlaveUserName, pConfigItem->szMqttSlavePassWord, pConfigItem->szMqttSlaveFilter);
	if (pMqttDef == NULL)
	{
		g_SynThreadCount--;
		return 1;
	}
	pMqttDef->pMqttTunnelItem = pConfigItem;
	//
	pMqttDef->ReadCSVFile(pConfigItem->TunnelID);
	//
	for (DWORD i = 0; i < pConfigItem->list.size(); i++)
	{
		pMqttDef->m_MqttMapTable1.SetAt(pConfigItem->list[i].szSrcTopic, &(pConfigItem->list[i]));
		pMqttDef->m_MqttMapTable2.SetAt(pConfigItem->list[i].szDstOPCPath, &(pConfigItem->list[i]));
		pMqttDef->m_MqttMapTable3.SetAt(pConfigItem->list[i].id, &(pConfigItem->list[i]));
		CString szDstTopic = pConfigItem->list[i].szDstOPCPath;
		szDstTopic.Replace('.', '/');
		szDstTopic.Format("%s/%s", pConfigItem->list[i].no, szDstTopic.Mid(szDstTopic.ReverseFind('/') + 1));
		pMqttDef->m_MqttMapTable4.SetAt(szDstTopic, &(pConfigItem->list[i]));
	}
	//
	SOCKADDR_IN		m_SockStruct;
	int				rets = 0;
	WSADATA			wsaData;
	if (WSAStartup(MAKEWORD(2, 1), &wsaData))
	{
		LogEvent("Winsock无法初始化！");
		//
		delete pMqttDef;
		//
		g_SynThreadCount--;
		return -1;
	}
	//
	pMqttDef->m_ServerSock = INVALID_SOCKET;
	m_SockStruct.sin_family = AF_INET;                  //使用TCP/IP协议
	m_SockStruct.sin_port = htons(pConfigItem->nSlavePort);
	m_SockStruct.sin_addr.S_un.S_addr = inet_addr(pConfigItem->szSlaveHost);
	pMqttDef->nSubPort = pConfigItem->nSlavePort;
	pMqttDef->m_szIPAddr = pConfigItem->szSlaveHost;
	g_MqttTunnelList.push_back(pMqttDef);
	//
	int nHeatCount = 0;
	while (g_RunServrice>0)
	{
		int rc = pMqttDef->loop(); 
		if (rc) {
			LogEvent("SlaveTunnelThread pMqttDef->loop rc=%d", rc);
			pMqttDef->reconnect();
			Sleep(1000);
		}
		//
		if (pMqttDef->m_ServerSock == INVALID_SOCKET)
		{
			if (pMqttDef->m_SubSock != INVALID_SOCKET)
			{
				closesocket(pMqttDef->m_SubSock);
				pMqttDef->m_SubSock = INVALID_SOCKET;
			}
			//
			int nTryCount = 0;
			while (g_RunServrice>0 && pMqttDef->bSubFlag == 1)
			{
				LogEvent("pMqttDef->bSubFlag=%d！", pMqttDef->bSubFlag);
				Sleep(1000);
				nTryCount++;
				//
				if (nTryCount>10)
				{
					//g_RunServrice = 0;
					break;
				}
			}
			//
			pMqttDef->m_ServerSock = socket(PF_INET, SOCK_STREAM, 0);
			if (pMqttDef->m_ServerSock == INVALID_SOCKET)
			{
				LogEvent("无法创建MQTT隧道客户端Socket！");
				Sleep(1000);
				continue;
			}
			if (connect(pMqttDef->m_ServerSock, (LPSOCKADDR)&m_SockStruct, sizeof(m_SockStruct)) == SOCKET_ERROR)
			{
				LogEvent("无法连接到MQTT隧道服务端(%s:%d)！", pConfigItem->szSlaveHost, pConfigItem->nSlavePort);
				closesocket(pMqttDef->m_ServerSock);
				pMqttDef->m_ServerSock = INVALID_SOCKET;
				Sleep(1000);
				continue;
			}
			//准备重新注册MQTT隧道服务
			if (pMqttDef->RegServer() == -1)
			{
				LogEvent("注册MQTT隧道服务(%s)失败！", pConfigItem->szName);
				closesocket(pMqttDef->m_ServerSock);
				pMqttDef->m_ServerSock = INVALID_SOCKET;
				Sleep(1000 * 10);
				continue;
			}
			Sleep(1000);
			//
			HANDLE				hThread;
			DWORD				dwThread;
			hThread = CreateThread(NULL, 0, SubTunnelThread, (LPVOID)pMqttDef, 0, &dwThread);
			if (hThread == NULL)
			{
				LogEvent("创建MQTT隧道服务子线程异常!");
			}
			else
			{
				g_SynThreadCount++;
				CloseHandle(hThread);
			}
		}
		//
		if (nHeatCount % 1000 == 0)
		{
			int nHeatCount = 0;
			if (pMqttDef->HeatServer() == -1)
			{
				struct MapValueItem myValue;
				memset(&myValue, 0, sizeof(struct MapValueItem));
				myValue.TunnelID = pMqttDef->pMqttTunnelItem->TunnelID;
				myValue.ValueID = 1;
				myValue.nUpFlag = 0;
				myValue.nDataType = 0;
				myValue.fDataValue = (rc == 0) ? 1 : 0;
				sprintf(myValue.szNodePath, "*");
				//
				pMqttDef->SendDataToMeter(&myValue);
				//
				LogEvent("MQTT隧道服务(%s)心跳检测失败！", pConfigItem->szName);
				closesocket(pMqttDef->m_ServerSock);
				pMqttDef->m_ServerSock = INVALID_SOCKET;
				Sleep(1000 * 10);
				continue;
			}
			else
			{
				if (rc == 0)
				{
					IoTServerStatus sStstaus;
					memset(&sStstaus, 0, sizeof(IoTServerStatus));
					strcpy(sStstaus.szIoTName, "MqttTunnel");
					sStstaus.nProcessID = GetCurrentProcessId();
					sStstaus.nRunStat = 1;
					//
					SendStatusToMonit(&sStstaus);
				}
				struct MapValueItem myValue;
				memset(&myValue, 0, sizeof(struct MapValueItem));
				myValue.TunnelID = pMqttDef->pMqttTunnelItem->TunnelID;
				myValue.ValueID = 1;
				myValue.nUpFlag = 1;
				myValue.nDataType = 0;
				myValue.fDataValue = (rc == 0) ? 1 : 0;
				sprintf(myValue.szNodePath, "*");
				//
				pMqttDef->SendDataToMeter(&myValue);
			}
		}
		else
		{
			nHeatCount++;
		}
	}
	//
	if (pMqttDef->m_SubSock != INVALID_SOCKET)
	{
		closesocket(pMqttDef->m_SubSock);
		pMqttDef->m_SubSock = INVALID_SOCKET;
	}
	//
	if (pMqttDef->m_ServerSock != INVALID_SOCKET)
	{
		closesocket(pMqttDef->m_ServerSock);
		pMqttDef->m_ServerSock = INVALID_SOCKET;
	}
	//
	int nTryCount = 0;
	while (pMqttDef->bSubFlag == 1)
	{
		Sleep(1000);
		if (nTryCount>10)
		{
			//g_RunServrice = 0;
			break;
		}
	}
	//
	delete pMqttDef;
	//
	g_SynThreadCount--;
	return 0;
}

DWORD WINAPI MqttTunnelThread(LPVOID lpParam)
{
	struct MqttTunnelItem	*pConfigItem = (struct MqttTunnelItem	*)lpParam;
	//
	TCHAR sMqttID[64];
	sprintf(sMqttID, "MQTT-%lu-MQTT", pConfigItem->TunnelID);
	CMqttTunnel *pMqttDef = new CMqttTunnel(sMqttID, pConfigItem->szMqttSlaveHost, pConfigItem->nMqttSlavePort, pConfigItem->szMqttSlaveUserName, pConfigItem->szMqttSlavePassWord, pConfigItem->szMqttSlaveFilter);
	if (pMqttDef == NULL)
	{
		g_SynThreadCount--;
		return 1;
	}
	pMqttDef->pMqttTunnelItem = pConfigItem;
	//
	pMqttDef->ReadCSVFile(pConfigItem->TunnelID);
	//
	for (DWORD i = 0; i < pConfigItem->list.size(); i++)
	{
		pMqttDef->m_MqttMapTable1.SetAt(pConfigItem->list[i].szSrcTopic, &(pConfigItem->list[i]));
		CString szDstTopic = pConfigItem->list[i].szDstOPCPath;
		szDstTopic.Replace('.','/');
		pMqttDef->m_MqttMapTable2.SetAt(szDstTopic, &(pConfigItem->list[i]));
		pMqttDef->m_MqttMapTable3.SetAt(pConfigItem->list[i].id, &(pConfigItem->list[i]));
		szDstTopic.Format("%s/%s", pConfigItem->list[i].no, szDstTopic.Mid(szDstTopic.ReverseFind('/') + 1));
		pMqttDef->m_MqttMapTable4.SetAt(szDstTopic, &(pConfigItem->list[i]));
	}
	//
	CMqttSmart *pMqttSmart = new CMqttSmart(sMqttID, pConfigItem->szMqttHost, pConfigItem->nMqttPort, pConfigItem->szMqttUserName, pConfigItem->szMqttPassWord);
	if (pMqttSmart == NULL)
	{
		delete pMqttDef;
		g_SynThreadCount--;
		return 2;
	}
	pMqttSmart->pMqttTunnelItem = pConfigItem;
	//
	for (DWORD i = 0; i < pConfigItem->list.size(); i++)
	{
		CString szDstTopic = pConfigItem->list[i].szDstOPCPath;
		szDstTopic.Replace('.', '/');
		pMqttSmart->m_MqttMapTable1.SetAt(szDstTopic, &(pConfigItem->list[i]));
		szDstTopic.Format("%s/%s", pConfigItem->list[i].no, szDstTopic.Mid(szDstTopic.ReverseFind('/') + 1));
		pMqttSmart->m_MqttMapTable2.SetAt(szDstTopic, &(pConfigItem->list[i]));
	}
	//
	pMqttDef->m_pMqttHandle = pMqttSmart;
	pMqttSmart->m_pMqttHandle = pMqttDef;
	g_MqttTunnelList.push_back(pMqttDef);
	//
	int nHeatCount = 0;
	while (g_RunServrice > 0)
	{
		int rc1 = pMqttDef->loop();
		if (rc1) {
			pMqttDef->reconnect();
			Sleep(1000);
		}
		//
		if (stricmp(pConfigItem->szDirect, "Both") == 0 || stricmp(pConfigItem->szDirect, "In") == 0)
		{
			int rc2 = pMqttSmart->loop();
			if (rc2) {
				struct MapValueItem myValue;
				memset(&myValue, 0, sizeof(struct MapValueItem));
				myValue.TunnelID = pMqttDef->pMqttTunnelItem->TunnelID;
				myValue.ValueID = 2;
				myValue.nUpFlag = 0;
				myValue.nDataType = 0;
				myValue.fDataValue = (rc1 == 0) ? 1 : 0;
				sprintf(myValue.szNodePath, "*");
				//
				pMqttDef->SendDataToMeter(&myValue);
				nHeatCount = 0;
				//
				pMqttSmart->m_bConnectFlag = 0;
				pMqttSmart->reconnect();
			}
			else
			{
				if (nHeatCount % 1000 == 0)
				{
					if (rc1 == 0)
					{
						IoTServerStatus sStstaus;
						memset(&sStstaus, 0, sizeof(IoTServerStatus));
						strcpy(sStstaus.szIoTName, "MqttTunnel");
						sStstaus.nProcessID = GetCurrentProcessId();
						sStstaus.nRunStat = 1;
						//
						SendStatusToMonit(&sStstaus);
					}
					struct MapValueItem myValue;
					memset(&myValue, 0, sizeof(struct MapValueItem));
					myValue.TunnelID = pMqttDef->pMqttTunnelItem->TunnelID;
					myValue.ValueID = 2;
					myValue.nUpFlag = 1;
					myValue.nDataType = 0;
					myValue.fDataValue = (rc1 == 0) ? 1 : 0;
					sprintf(myValue.szNodePath, "*");
					//
					pMqttDef->SendDataToMeter(&myValue);
					nHeatCount = 0;
				}
				else
				{
					nHeatCount++;
				}
			}
		}
		else
		{
			Sleep(50);
		}
	}
	//
	delete pMqttSmart;
	delete pMqttDef;
	//
	g_SynThreadCount--;
	return 0;
}

DWORD WINAPI KafkaTunnelThread(LPVOID lpParam)
{
	struct MqttTunnelItem	*pConfigItem = (struct MqttTunnelItem	*)lpParam;
	//
	TCHAR sMqttID[64];
	sprintf(sMqttID, "KAFKA-%lu-MQTT", pConfigItem->TunnelID);
	CMqttTunnel *pMqttDef = new CMqttTunnel(sMqttID, pConfigItem->szMqttSlaveHost, pConfigItem->nMqttSlavePort, pConfigItem->szMqttSlaveUserName, pConfigItem->szMqttSlavePassWord, pConfigItem->szMqttSlaveFilter);
	if (pMqttDef == NULL)
	{
		g_SynThreadCount--;
		return 1;
	}
	pMqttDef->pMqttTunnelItem = pConfigItem;
	//
	pMqttDef->ReadCSVFile(pConfigItem->TunnelID);
	//
	for (DWORD i = 0; i < pConfigItem->list.size(); i++)
	{
		pMqttDef->m_MqttMapTable1.SetAt(pConfigItem->list[i].szSrcTopic, &(pConfigItem->list[i]));
		CString szDstTopic = pConfigItem->list[i].szDstOPCPath;
		szDstTopic.Replace('.', '/');
		pMqttDef->m_MqttMapTable2.SetAt(szDstTopic, &(pConfigItem->list[i]));
		pMqttDef->m_MqttMapTable3.SetAt(pConfigItem->list[i].id, &(pConfigItem->list[i]));
		szDstTopic.Format("%s/%s", pConfigItem->list[i].no, szDstTopic.Mid(szDstTopic.ReverseFind('/') + 1));
		pMqttDef->m_MqttMapTable4.SetAt(szDstTopic, &(pConfigItem->list[i]));
	}
	//
	ConsummerKafka*	pConsummerKafka = new ConsummerKafka();
	if (pConsummerKafka == NULL)
	{
		delete pMqttDef;
		g_SynThreadCount--;
		return 2;
	}
	pConsummerKafka->pMqttTunnelItem = pConfigItem;
	//
	for (DWORD i = 0; i < pConfigItem->list.size(); i++)
	{
		CString szDstTopic = pConfigItem->list[i].szDstOPCPath;
		szDstTopic.Replace('.', '/');
		pConsummerKafka->m_MqttMapTable1.SetAt(szDstTopic, &(pConfigItem->list[i]));
		szDstTopic.Format("%s/%s", pConfigItem->list[i].no, szDstTopic.Mid(szDstTopic.ReverseFind('/') + 1));
		pConsummerKafka->m_MqttMapTable2.SetAt(szDstTopic, &(pConfigItem->list[i]));
	}
	pMqttDef->m_pConsummerHandle = pConsummerKafka;
	pConsummerKafka->m_pMqttHandle = pMqttDef;
	//
	ProducerKafka *pProducerKafka = new ProducerKafka();
	if (pProducerKafka == NULL)
	{
		delete pMqttDef;
		g_SynThreadCount--;
		return 2;
	}
	pProducerKafka->pMqttTunnelItem = pConfigItem;
	//
	//
	pMqttDef->m_pKafkaHandle = pProducerKafka;
	pProducerKafka->m_pMqttHandle = pMqttDef;
	g_MqttTunnelList.push_back(pMqttDef);
	//
	int nHeatCount = 0;
	while (g_RunServrice > 0)
	{
		int rc1 = pMqttDef->loop();
		if (rc1) {
			pMqttDef->reconnect();
			Sleep(1000);
		}
		//
		if ((stricmp(pConfigItem->szDirect, "Both") == 0 || stricmp(pConfigItem->szDirect, "In") == 0)
			&& strlen(pConfigItem->szConsumerTopic)>0)
		{
			if (!pConsummerKafka->m_bConnectFlag)
			{
				if (PRODUCER_INIT_SUCCESS == pConsummerKafka->init_kafka(pConfigItem->szKafkaBroker, pConfigItem->szKafkaUserName, pConfigItem->szKafkaPassWord, sMqttID, pConfigItem->szConsumerTopic))
				{
					struct MapValueItem myValue;
					memset(&myValue, 0, sizeof(struct MapValueItem));
					myValue.TunnelID = pMqttDef->pMqttTunnelItem->TunnelID;
					myValue.ValueID = 3;
					myValue.nUpFlag = 1;
					myValue.nDataType = 0;
					myValue.fDataValue = (rc1 == 0) ? 1 : 0;
					sprintf(myValue.szNodePath, "*");
					pMqttDef->SendDataToMeter(&myValue);
					pConsummerKafka->m_bConnectFlag = 1;
				}
				else
				{
					struct MapValueItem myValue;
					memset(&myValue, 0, sizeof(struct MapValueItem));
					myValue.TunnelID = pMqttDef->pMqttTunnelItem->TunnelID;
					myValue.ValueID = 3;
					myValue.nUpFlag = 0;
					myValue.nDataType = 0;
					myValue.fDataValue = (rc1 == 0) ? 1 : 0;
					sprintf(myValue.szNodePath, "*");
					pMqttDef->SendDataToMeter(&myValue);
					pConsummerKafka->m_bConnectFlag = 0;
				}
			}
			else
			{
				if (rc1 == 0)
				{
					IoTServerStatus sStstaus;
					memset(&sStstaus, 0, sizeof(IoTServerStatus));
					strcpy(sStstaus.szIoTName, "MqttTunnel");
					sStstaus.nProcessID = GetCurrentProcessId();
					sStstaus.nRunStat = 1;
					//
					SendStatusToMonit(&sStstaus);
				}
				//
				int rc2 = pConsummerKafka->pull_data_from_kafka();
				if (rc2 == -1)
				{
					if (nHeatCount > 10)
					{
						pConsummerKafka->destroy();
						pConsummerKafka->m_bConnectFlag = 0;
						nHeatCount = 0;
					}
					else
					{
						nHeatCount++;
					}
				}
				else
				{
					nHeatCount = 0;
				}
				//
				struct MapValueItem myValue;
				memset(&myValue, 0, sizeof(struct MapValueItem));
				myValue.TunnelID = pMqttDef->pMqttTunnelItem->TunnelID;
				myValue.ValueID = 3;
				myValue.nUpFlag = pConsummerKafka->m_bConnectFlag;
				myValue.nDataType = 0;
				myValue.fDataValue = (rc1 == 0) ? 1 : 0;
				sprintf(myValue.szNodePath, "*");
				pMqttDef->SendDataToMeter(&myValue);
			}
		}
		else
		{
			Sleep(50);
		}
	}
	//
	delete pConsummerKafka;
	delete pProducerKafka;
	delete pMqttDef;
	//
	g_SynThreadCount--;
	return 0;
}

DWORD WINAPI AmqpTunnelThread(LPVOID lpParam)
{
	struct MqttTunnelItem	*pConfigItem = (struct MqttTunnelItem	*)lpParam;
	//
	TCHAR sMqttID[64];
	sprintf(sMqttID, "AMQP-%lu-MQTT", pConfigItem->TunnelID);
	CMqttTunnel *pMqttDef = new CMqttTunnel(sMqttID, pConfigItem->szMqttSlaveHost, pConfigItem->nMqttSlavePort, pConfigItem->szMqttSlaveUserName, pConfigItem->szMqttSlavePassWord, pConfigItem->szMqttSlaveFilter);
	if (pMqttDef == NULL)
	{
		g_SynThreadCount--;
		return 1;
	}
	pMqttDef->pMqttTunnelItem = pConfigItem;
	//
	pMqttDef->ReadCSVFile(pConfigItem->TunnelID);
	//
	for (DWORD i = 0; i < pConfigItem->list.size(); i++)
	{
		pMqttDef->m_MqttMapTable1.SetAt(pConfigItem->list[i].szSrcTopic, &(pConfigItem->list[i]));
		CString szDstTopic = pConfigItem->list[i].szDstOPCPath;
		szDstTopic.Replace('.', '/');
		pMqttDef->m_MqttMapTable2.SetAt(szDstTopic, &(pConfigItem->list[i]));
		pMqttDef->m_MqttMapTable3.SetAt(pConfigItem->list[i].id, &(pConfigItem->list[i]));
		szDstTopic.Format("%s/%s", pConfigItem->list[i].no, szDstTopic.Mid(szDstTopic.ReverseFind('/') + 1));
		pMqttDef->m_MqttMapTable4.SetAt(szDstTopic, &(pConfigItem->list[i]));
	}
	//
	string err;
	CAmqpSmart *pAmqpSmart = new CAmqpSmart(pConfigItem->szAmqpHost, pConfigItem->nAmqpPort, pConfigItem->szAmqpUserName, pConfigItem->szAmqpPassWord);
	if (pAmqpSmart == NULL)
	{
		delete pMqttDef;
		g_SynThreadCount--;
		return 2;
	}
	pAmqpSmart->pMqttTunnelItem = pConfigItem;
	//
	for (DWORD i = 0; i < pConfigItem->list.size(); i++)
	{
		CString szDstTopic = pConfigItem->list[i].szDstOPCPath;
		szDstTopic.Replace('.', '/');
		pAmqpSmart->m_MqttMapTable1.SetAt(szDstTopic, &(pConfigItem->list[i]));
		szDstTopic.Format("%s/%s", pConfigItem->list[i].no, szDstTopic.Mid(szDstTopic.ReverseFind('/') + 1));
		pAmqpSmart->m_MqttMapTable2.SetAt(szDstTopic, &(pConfigItem->list[i]));
	}
	//
	pMqttDef->m_pAmqpHandle = pAmqpSmart;
	pAmqpSmart->m_pMqttHandle = pMqttDef;
	g_MqttTunnelList.push_back(pMqttDef);
	//
	int nHeatCount = 0;
	while (g_RunServrice > 0)
	{
		int rc1 = pMqttDef->loop();
		if (rc1) {
			pMqttDef->reconnect();
			Sleep(1000);
		}
		//
		if (!pAmqpSmart->m_bConnectFlag)
		{
			struct MapValueItem myValue;
			memset(&myValue, 0, sizeof(struct MapValueItem));
			myValue.TunnelID = pMqttDef->pMqttTunnelItem->TunnelID;
			myValue.ValueID = 4;
			myValue.nUpFlag = 0;
			myValue.nDataType = 0;
			myValue.fDataValue = (rc1 == 0) ? 1 : 0;
			sprintf(myValue.szNodePath, "*");
			pMqttDef->SendDataToMeter(&myValue);
			//
			CExchange exchange(pConfigItem->szAmqpExchange);
			string queue_name(pConfigItem->szAmqpQueue);
			CQueue queue_temp(queue_name);
			//
			if (pAmqpSmart->Connect(err)<0)
			{
				LogEvent("连接AMQP服务失败，%s", err.c_str());
				Sleep(1000);
				continue;
			}
			if (pAmqpSmart->exchange_declare(exchange, err) < 0)
			{
				LogEvent("发布AMQP消息失败，声明交换机失败，%s", err.c_str());
				Sleep(1000);
				continue;
			}
			//声明一个队列
			if ((pAmqpSmart->queue_declare(queue_temp, err) < 0))
			{
				LogEvent("发布AMQP消息失败，声明队列失败，%s", err.c_str());
				Sleep(1000);
				continue;
			}
			//将交换机绑定到队列， 
			if ((pAmqpSmart->queue_bind(queue_temp, exchange, queue_name, err)<0))
			{
				LogEvent("发布AMQP消息失败，交换机绑定到队列失败，%s", err.c_str());
				Sleep(1000);
				continue;
			}
			pAmqpSmart->m_bConnectFlag = 1;
		}
		else
		{
			if (rc1 == 0)
			{
				IoTServerStatus sStstaus;
				memset(&sStstaus, 0, sizeof(IoTServerStatus));
				strcpy(sStstaus.szIoTName, "MqttTunnel");
				sStstaus.nProcessID = GetCurrentProcessId();
				sStstaus.nRunStat = 1;
				//
				SendStatusToMonit(&sStstaus);
			}
			//
			if (stricmp(pConfigItem->szDirect, "Both") == 0 || stricmp(pConfigItem->szDirect, "In") == 0)
			{
				int rc2 = pAmqpSmart->subscribe();
				if (rc2 == -1)
				{
					if (nHeatCount > 10)
					{
						pAmqpSmart->Disconnect();
						pAmqpSmart->m_bConnectFlag = 0;
						nHeatCount = 0;
					}
					else
					{
						nHeatCount++;
					}
				}
				else
				{
					nHeatCount = 0;
				}
				//
				struct MapValueItem myValue;
				memset(&myValue, 0, sizeof(struct MapValueItem));
				myValue.TunnelID = pMqttDef->pMqttTunnelItem->TunnelID;
				myValue.ValueID = 4;
				myValue.nUpFlag = pAmqpSmart->m_bConnectFlag;
				myValue.nDataType = 0;
				myValue.fDataValue = (rc1 == 0) ? 1 : 0;
				sprintf(myValue.szNodePath, "*");
				pMqttDef->SendDataToMeter(&myValue);
			}
			else
			{
				Sleep(50);
			}
		}
	}
	//
	delete pAmqpSmart;
	delete pMqttDef;
	//
	g_SynThreadCount--;
	return 0;
}
//change   the   privilege   of   this   process.   
BOOL   EnablePrivilege(LPCTSTR   lpszPrivilegeName,BOOL   bEnable)   
{   
	HANDLE   hToken;   
	TOKEN_PRIVILEGES   tkp;   

	if   (!OpenProcessToken(   GetCurrentProcess(),TOKEN_ADJUST_PRIVILEGES   |   TOKEN_QUERY,   &hToken   )   )   
		return   FALSE;   
	if   (!LookupPrivilegeValue(NULL,lpszPrivilegeName,&   tkp.Privileges[0].Luid))   
	{   
		CloseHandle(hToken);   
		return   FALSE;   
	}   
	tkp.PrivilegeCount   =   1;   
	tkp.Privileges[0].Attributes   =bEnable?SE_PRIVILEGE_ENABLED:0;   
	if   (   !   AdjustTokenPrivileges(hToken,   FALSE,   &tkp,   sizeof   tkp,   NULL,   NULL))   
	{   
		CloseHandle(hToken);   
		return   FALSE;   
	}   
	CloseHandle(hToken);   
	return   TRUE;   
}   
//Shutdown,reboot,logout   func   
BOOL   ShutDownSysEx(UINT   uFlags,BOOL   bForce)   
{   
	BOOL   bRet=TRUE;   

	if(   uFlags   &   SD_SCREENSAVE   )   
	{   
		return   ::SendMessage(HWND_BROADCAST,WM_SYSCOMMAND,SC_SCREENSAVE,0);   
	}   

	if(   uFlags   &   SD_MONITOR   )   
	{   
		DWORD   lParam=bForce?   2:1;   
		return   ::SendMessage(HWND_BROADCAST,WM_SYSCOMMAND,SC_MONITORPOWER,lParam);   
	}   

	if(   uFlags   &   SD_SUSPEND   )   
	{   
		EnablePrivilege(SE_SHUTDOWN_NAME,TRUE);   
		//suspend   system   
		bRet   =   SetSystemPowerState(TRUE,bForce);   
		EnablePrivilege(SE_SHUTDOWN_NAME,FALSE);   
	}   
	else   
	{   
		bForce?uFlags|=EWX_FORCE:uFlags&=~EWX_FORCE;   

		//enable   the     privilege   of   shutdow   in   win2000   or   later   
		EnablePrivilege(SE_SHUTDOWN_NAME,TRUE);   

		if(!ExitWindowsEx(   uFlags,0   ))   
		{   
			EnablePrivilege(SE_SHUTDOWN_NAME,FALSE);   
			bRet   =   FALSE;   
		}   
	}   
	return   bRet;   
}   

//////////////////////////////////////////////////////////////////////
// Construction/Destruction
//////////////////////////////////////////////////////////////////////

CService::CService()
{
	
}

CService::~CService()
{

}

void CService::Init(LPCTSTR pServiceName,LPCTSTR pServiceDisplayedName)
{
    lstrcpy(m_szServiceName,pServiceName);
    lstrcpy(m_szServiceDisplayedName,pServiceDisplayedName);

    // set up the initial service status 
    m_hServiceStatus = NULL;
    m_status.dwServiceType = SERVICE_WIN32_OWN_PROCESS;
    m_status.dwCurrentState = SERVICE_STOPPED;
    m_status.dwControlsAccepted = SERVICE_ACCEPT_STOP;
    m_status.dwWin32ExitCode = 0;
    m_status.dwServiceSpecificExitCode = 0;
    m_status.dwCheckPoint = 0;
    m_status.dwWaitHint = 0;
}

void CService::Start(PCHAR	 pszTunnelName, DWORD nTunnelID)
{
    SERVICE_TABLE_ENTRY st[] =
    {
		{ m_szServiceName, _ServiceMain },
        { NULL, NULL }
    };
    if (!::StartServiceCtrlDispatcher(st) && m_bService)
	{
		DWORD dw = GetLastError();
		LogEvent("StartServiceCtrlDispatcher Error=%d",dw);
		m_bService = FALSE;
	}

    if (m_bService == FALSE)
	{
		g_RunServrice=1;
		Run(pszTunnelName, nTunnelID);
	}
}

void CService::ServiceMain()
{
    // Register the control request handler
    m_status.dwCurrentState = SERVICE_START_PENDING;
    m_hServiceStatus = RegisterServiceCtrlHandler(m_szServiceName, _Handler);
    if (m_hServiceStatus == NULL)
    {
        LogEvent("Handler not installed");
        return;
    }
    SetServiceStatus(SERVICE_START_PENDING);

    m_status.dwWin32ExitCode = S_OK;
    m_status.dwCheckPoint = 0;
    m_status.dwWaitHint = 0;

    // When the Run function returns, the service has stopped.
	g_RunServrice=2;
    Run(NULL,0);

    LogEvent("MqttTunnel Service stopped 1");
    SetServiceStatus(SERVICE_STOPPED);
	g_RunServrice=-1;
}

inline void CService::Handler(DWORD dwOpcode)
{
    switch (dwOpcode)
    {
    case SERVICE_CONTROL_STOP:
		LogEvent("MqttTunnel Request to stop...");
		SetServiceStatus(SERVICE_STOP_PENDING);
		g_RunServrice=0;
        break;
    case SERVICE_CONTROL_PAUSE:
        break;
    case SERVICE_CONTROL_CONTINUE:
        break;
    case SERVICE_CONTROL_INTERROGATE:
        break;
    case SERVICE_CONTROL_SHUTDOWN:
        break;
    default:
		break;
    }
}

void WINAPI CService::_ServiceMain(DWORD dwArgc, LPTSTR* lpszArgv)
{
    _Module.ServiceMain();
}
void WINAPI CService::_Handler(DWORD dwOpcode)
{
    _Module.Handler(dwOpcode); 
}

void CService::SetServiceStatus(DWORD dwState)
{
    m_status.dwCurrentState = dwState;
    ::SetServiceStatus(m_hServiceStatus, &m_status);
}


void CService::Run(PCHAR	 pszTunnelName, DWORD nTunnelID)
{
	int keepalive = 60;
	mosqpp::lib_init();
	g_hMqttWriteMutex = CreateMutex(NULL, FALSE, NULL);
	char clientId[64];
	if (pszTunnelName == NULL)
	{
		sprintf_s(clientId, "mqtt_%s_debug", (CTime::GetCurrentTime()).Format("%Y%m%d%H%M%S"));
	}
	else
	{
		sprintf_s(clientId, "mqtt_%s_%lu", (CTime::GetCurrentTime()).Format("%Y%m%d%H%M%S"), nTunnelID);
	}
	g_pMqttPub = new mosqpp::mosquittopp(clientId);
	g_pMqttPub->username_pw_set("ThingsBox", "box999");
	if (g_pMqttPub->connect("localhost", 1883, keepalive) == 0)
		g_bMqttConnect = TRUE;
	else
		g_bMqttConnect = FALSE;
	//
	LogEvent("MqttTunnel Service started:%d",m_bService);
	m_dwThreadID = GetCurrentThreadId();

    if (m_bService)
        SetServiceStatus(SERVICE_RUNNING);
	//
	DWORD dwSize = GetModuleFileName(NULL,g_ServricePath,8192);
	g_ServricePath[dwSize] = 0;
	if(dwSize>4&&g_ServricePath[dwSize-4]=='.')
	{
		g_ServricePath[dwSize-14] = 0;
	}
	//
	strcpy(g_DebugLogPath, g_ServricePath);
	g_DebugLogPath[dwSize - 30] = 0;
	//
	WSADATA wsaData;
	if (WSAStartup(MAKEWORD(2, 1), &wsaData))
	{
		LogEvent("Winsock无法初始化!");
		WSACleanup();
	}
	//
	if(ReadConfig(pszTunnelName, nTunnelID)==0)
	{
		if (pszTunnelName == NULL)
		{
			JOBOBJECT_EXTENDED_LIMIT_INFORMATION limit;
			FcpJobObject = (HANDLE)CreateJobObject(NULL, NULL);
			if (FcpJobObject == NULL)
				exit(1);
			//
			/* let all processes assigned to this job object
			* being killed when the job object closed */
			if (!QueryInformationJobObject(FcpJobObject, JobObjectExtendedLimitInformation, &limit, sizeof(limit), NULL)) {
				CloseHandle(FcpJobObject);
				exit(1);
			}

			limit.BasicLimitInformation.LimitFlags |= JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;

			if (!SetInformationJobObject(FcpJobObject, JobObjectExtendedLimitInformation, &limit, sizeof(limit))) {
				CloseHandle(FcpJobObject);
				exit(1);
			}
			//
			for (u_int32_t i = 0; i < m_ProcessList.size(); i++)
			{
				HANDLE				hThread;
				DWORD				dwThread;
				hThread = CreateThread(NULL, 0, spawn_process, (LPVOID)&m_ProcessList[i], 0, &dwThread);
				if (hThread == NULL)
				{
					LogEvent("无法创建子进程！");
				}
				else
				{
					g_SynThreadCount++;
					CloseHandle(hThread);
					//
					struct ProcessStatusItem status;
					memset(&status, 0, sizeof(struct ProcessStatusItem));
					status.nStatus = 3;
					status.nTunnelID = m_ProcessList[i].nTunnelID;
					strcpy(status.szType, "MQTT");
					m_StatusList.push_back(status);
				}
			}
			//
			WriteTunnelStatus();
		}
		else
		{
			for (u_int32_t i = 0; i < m_ConfigList.size(); i++)
			{
				if (stricmp(m_ConfigList[i].szType, "ThingsDataLink") == 0)
				{
					if (strlen(m_ConfigList[i].szMasteHost) > 0)
					{
						HANDLE				hThread;
						DWORD				dwThread;
						//
						hThread = CreateThread(NULL, 0, MasteTunnelThread, (LPVOID)&m_ConfigList[i], 0, &dwThread);
						if (hThread == NULL)
						{
							LogEvent("创建SOCKET服务端线程异常!");
						}
						else
						{
							g_SynThreadCount++;
							CloseHandle(hThread);
						}
					}
					//
					if (strlen(m_ConfigList[i].szSlaveHost) > 0)
					{
						HANDLE				hThread;
						DWORD				dwThread;
						//
						hThread = CreateThread(NULL, 0, SlaveTunnelThread, (LPVOID)&m_ConfigList[i], 0, &dwThread);
						if (hThread == NULL)
						{
							LogEvent("创建SOCKET服务端线程异常!");
						}
						else
						{
							g_SynThreadCount++;
							CloseHandle(hThread);
						}
					}
				}
				//
				else if (stricmp(m_ConfigList[i].szType, "MQTT") == 0)
				{
					if (strlen(m_ConfigList[i].szMqttHost) > 0)
					{
						HANDLE				hThread;
						DWORD				dwThread;
						//
						hThread = CreateThread(NULL, 0, MqttTunnelThread, (LPVOID)&m_ConfigList[i], 0, &dwThread);
						if (hThread == NULL)
						{
							LogEvent("创建SOCKET服务端线程异常!");
						}
						else
						{
							g_SynThreadCount++;
							CloseHandle(hThread);
						}
					}
				}
				//
				else if (stricmp(m_ConfigList[i].szType, "AMQP") == 0)
				{
					if (strlen(m_ConfigList[i].szAmqpHost) > 0)
					{
						HANDLE				hThread;
						DWORD				dwThread;
						//
						hThread = CreateThread(NULL, 0, AmqpTunnelThread, (LPVOID)&m_ConfigList[i], 0, &dwThread);
						if (hThread == NULL)
						{
							LogEvent("创建SOCKET服务端线程异常!");
						}
						else
						{
							g_SynThreadCount++;
							CloseHandle(hThread);
						}
					}
				}
				//
				else if (stricmp(m_ConfigList[i].szType, "Kafka") == 0)
				{
					if (strlen(m_ConfigList[i].szKafkaBroker) > 0)
					{
						HANDLE				hThread;
						DWORD				dwThread;
						//
						hThread = CreateThread(NULL, 0, KafkaTunnelThread, (LPVOID)&m_ConfigList[i], 0, &dwThread);
						if (hThread == NULL)
						{
							LogEvent("创建SOCKET服务端线程异常!");
						}
						else
						{
							g_SynThreadCount++;
							CloseHandle(hThread);
						}
					}
				}
			}
		}
	}
	else
	{
		LogEvent("读取配置文件失败!");
		return;
	}
	/*
	*	等待结束信号
	*/
	DWORD nCount = 1;
	while(g_RunServrice>0)
	{
		if (pszTunnelName == NULL)
		{
			if (nCount % 5 == 0
				&& ReadTunnelStatus() == 0)
			{
				BOOL bUpdateFlag = FALSE;
				for (int i = 0; i < m_StatusList.size(); i++)
				{
					if (m_StatusList[i].nStatus == 0)
					{
						ReadConfig(NULL, 0);
						//
						for (u_int32_t j = 0; j < m_ProcessList.size(); j++)
						{
							if (m_ProcessList[j].nTunnelID == m_StatusList[i].nTunnelID)
							{
								HANDLE				hThread;
								DWORD				dwThread;
								hThread = CreateThread(NULL, 0, spawn_process, (LPVOID)&m_ProcessList[j], 0, &dwThread);
								if (hThread == NULL)
								{
									LogEvent("无法创建子进程！");
								}
								else
								{
									g_SynThreadCount++;
									CloseHandle(hThread);
									//
									m_StatusList[i].nStatus = 3;
									bUpdateFlag = TRUE;
								}
								break;
							}
						}
					}
					else if (m_StatusList[i].nStatus == 1)
					{
						for (u_int32_t j = 0; j < m_ProcessList.size(); j++)
						{
							if (m_ProcessList[j].nTunnelID == m_StatusList[i].nTunnelID)
							{
								TerminateProcess(m_ProcessList[j].hProcess, 1);
								//
								m_StatusList[i].nStatus = 4;
								bUpdateFlag = TRUE;
								break;
							}
						}
					}
					else if (m_StatusList[i].nStatus == 2)
					{
						for (u_int32_t j = 0; j < m_ProcessList.size(); j++)
						{
							if (m_ProcessList[j].nTunnelID == m_StatusList[i].nTunnelID)
							{
								TerminateProcess(m_ProcessList[j].hProcess, 1);
								//
								m_StatusList.erase(m_StatusList.begin() + i);
								i--;
								bUpdateFlag = TRUE;
								break;
							}
						}
						//
						ReadConfig(NULL, 0);
					}
				}
				//
				if (bUpdateFlag)
				{
					WriteTunnelStatus();
				}
			}
		}
		else
		{
			if (nCount % 60 == 0)
			{
				IoTServerStatus sStstaus;
				memset(&sStstaus, 0, sizeof(IoTServerStatus));
				strcpy(sStstaus.szIoTName, "MqttTunnel");
				sStstaus.nProcessID = GetCurrentProcessId();
				sStstaus.nRunStat = 2;
				SendStatusToMonit(&sStstaus);
			}
		}
		//
		Sleep(1000);
		nCount++;
	}	
	//
	if (pszTunnelName == NULL)
	{
		for (u_int32_t i = 0; i < m_ProcessList.size(); i++)
		{
			DWORD nExitCode = 0;
			if (m_ProcessList[i].hProcess != NULL
				&& TerminateProcess(m_ProcessList[i].hProcess, nExitCode))
			{
				LogEvent("正在关闭子进程 %s %d", m_ProcessList[i].szTunnelName, m_ProcessList[i].nTunnelID);
			}
		}
	}
	/*
	*	等待结束信号
	*/
	while (g_SynThreadCount>0)
	{
		Sleep(1000);
	}
	/*
	*	退出同步服务
	*/
	LogEvent("结束MQTT路由隧道服务!");
}

BOOL CService::Install()
{
    if (IsInstalled())
        return TRUE;

    SC_HANDLE hSCM = ::OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);
    if (hSCM == NULL)
    {
        MessageBox(NULL, "Couldn't open service manager", m_szServiceName, MB_OK);
        return FALSE;
    }

    // Get the executable file path
    TCHAR szFilePath[_MAX_PATH];
    ::GetModuleFileName(NULL, szFilePath, _MAX_PATH);

	DWORD dwStartupType = SERVICE_AUTO_START;
    SC_HANDLE hService = ::CreateService(
        hSCM, m_szServiceName, m_szServiceDisplayedName,
        SERVICE_ALL_ACCESS, SERVICE_WIN32_OWN_PROCESS | SERVICE_INTERACTIVE_PROCESS,
        dwStartupType, SERVICE_ERROR_NORMAL,
        szFilePath, NULL, NULL, NULL, NULL, NULL);

    if (hService == NULL)
    {
        ::CloseServiceHandle(hSCM);
        MessageBox(NULL, "Couldn't create service", m_szServiceName, MB_OK);
        return FALSE;
    }

	if (dwStartupType == SERVICE_AUTO_START)
		StartService(hService, 0, NULL);

    ::CloseServiceHandle(hService);
    ::CloseServiceHandle(hSCM);
    return TRUE;
}

BOOL CService::Uninstall(DWORD dwTimeout)
{
    if (!IsInstalled())
        return TRUE;

    SC_HANDLE hSCM = ::OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);

    if (hSCM == NULL)
    {
        MessageBox(NULL, "Couldn't open service manager", m_szServiceName, MB_OK);
        return FALSE;
    }

    SC_HANDLE hService = ::OpenService(hSCM, m_szServiceName, SERVICE_STOP | DELETE | SERVICE_QUERY_STATUS);

    if (hService == NULL)
    {
        ::CloseServiceHandle(hSCM);
        MessageBox(NULL, "Couldn't open service", m_szServiceName, MB_OK);
        return FALSE;
    }
    SERVICE_STATUS status = {0};
	DWORD dwStartTime = GetTickCount();

    if (ControlService(hService, SERVICE_CONTROL_STOP, &status))
	{
		// Wait for the service to stop
		while ( status.dwCurrentState != SERVICE_STOPPED )
		{
			Sleep( status.dwWaitHint );
			if ( !QueryServiceStatus( hService, &status ) )
				return FALSE;

			if ( status.dwCurrentState == SERVICE_STOPPED )
				break;

			if ( GetTickCount() - dwStartTime > dwTimeout )
			{
				MessageBox(NULL,"Service could not be stopped", NULL, MB_OK);
				return FALSE;
			}
		}
	}

    BOOL bDelete = ::DeleteService(hService);
    ::CloseServiceHandle(hService);
    ::CloseServiceHandle(hSCM);

    if (bDelete)
        return TRUE;

    MessageBox(NULL, "Service could not be deleted", m_szServiceName, MB_OK);
    return FALSE;
}

BOOL CService::IsInstalled()
{
    BOOL bResult = FALSE;

    SC_HANDLE hSCM = ::OpenSCManager(NULL, NULL, SC_MANAGER_ALL_ACCESS);

    if (hSCM != NULL)
    {
        SC_HANDLE hService = ::OpenService(hSCM, m_szServiceName, SERVICE_QUERY_CONFIG);
        if (hService != NULL)
        {
            bResult = TRUE;
            ::CloseServiceHandle(hService);
        }
        ::CloseServiceHandle(hSCM);
    }
    return bResult;
}


///////////////////////////////////////////////////////////////////////////////////////

LPTSTR CService::FormatMsg(DWORD dwError)
{
	LPTSTR lpszMsg=NULL;
	if(!FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_ALLOCATE_BUFFER,
		NULL, 
		dwError,
		MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US), 
		(LPTSTR)&lpszMsg,
		0,
		NULL))
	{
		memset(m_szFormatMsg,0,32);
		sprintf_s(m_szFormatMsg,"%u",dwError);
		return m_szFormatMsg;
	}
	return lpszMsg;
}

int CService::ReadTunnelStatus()
{
	char pFile[512];
	sprintf_s(pFile, "%sStat", g_ServricePath);
	FILE* pf1 = fopen(pFile, "rb");
	if (pf1 != NULL)
	{
		m_StatusList.clear();
		while (!feof(pf1))
		{
			struct ProcessStatusItem status;
			memset(&status, 0, sizeof(status));
			if (fread(&status, sizeof(status), 1, pf1) > 0)
			{
				m_StatusList.push_back(status);
			}
		}
		fclose(pf1);
		return 0;
	}
	return -1;
}
int CService::WriteTunnelStatus()
{
	char pFile[512];
	sprintf_s(pFile, "%sStat", g_ServricePath);
	FILE* pf = fopen(pFile, "wb");
	if (pf != NULL)
	{
		for (int i = 0; i < m_StatusList.size(); i++)
		{
			fwrite(&m_StatusList[i], sizeof(struct ProcessStatusItem), 1, pf);
		}
		fclose(pf);
		return 0;
	}
	return -1;
}


int CService::ReadConfig(PCHAR	 pszTunnelName, DWORD nTunnelID)
{
	char pModuleFile[8192];
	DWORD dwSize = GetModuleFileName(NULL,pModuleFile,8192);
	pModuleFile[dwSize] = 0;
	if(dwSize>4&&pModuleFile[dwSize-4]=='.')
	{
		pModuleFile[dwSize-14] = 0;
	}
	//
	CIniFile pFile;
	pFile.SetName("config.ini");
	pFile.SetPath(pModuleFile);
	BOOL bExist=pFile.OpenIniFileForRead();
	if(!bExist)
	{
		LogEvent("ReadConfig %sconfig.ini not Exist",pModuleFile);	
		return -1;
	}
	//
	CString szSection, szValue;
	int nValue, nTotalMqttTunnel = 0;
	if(!pFile.GetItemInt("Settings","TotalMqttTunnel", nTotalMqttTunnel))
		nTotalMqttTunnel = 0;
	//
	int	m_OffineSpan =0;
	if (pFile.GetItemInt("Settings", "OffineSpan", m_OffineSpan))
	{
		g_DebugMode = m_OffineSpan;
	}
	//
	int	m_OffineRecord = 0;
	if (pFile.GetItemInt("Settings", "OffineRecord", m_OffineRecord))
	{
		g_OffineRecord = m_OffineRecord;
	}
	//
	int	m_ActiveTime = 0;
	if (pFile.GetItemInt("Settings", "ActiveTime", m_ActiveTime))
	{
		g_ActiveTime = m_ActiveTime;
	}
	//
	int	m_DebugMode = 0;
	if (pFile.GetItemInt("Settings", "DebugMode", m_DebugMode))
	{
		g_DebugMode = m_DebugMode;
	}
	if (!pFile.GetItemString("Settings", "MachineNO", g_szMachineNO))
	{
		g_szMachineNO = "未授权设备";
	}
	if (pFile.GetItemString(szSection, "ApplyDesc", szValue))
	{
		g_szApplyDesc = szValue;
	}
	else
	{
		g_szApplyDesc = g_szMachineNO;
	}
	//
	for(int i=0;i<nTotalMqttTunnel;i++)
	{
		szSection.Format("MqttTunnel%d", i + 1);
		//
		struct MqttTunnelItem info;
		info.dataTime = 0;
		info.bUpStatus = 0;
		info.bDownStatus = 0;
		//
		if (!pFile.GetItemInt(szSection, "TunnelID", nValue))
		{
			LogEvent("ReadMqttTunnel #TunnelID");
			pFile.CloseIniFile();
			return -1;
		}
		info.TunnelID = nValue;
		//
		if (!pFile.GetItemString(szSection, "TunnelName", szValue))
		{
			LogEvent("ReadMqttTunnel #TunnelName");
			pFile.CloseIniFile();
			return -1;
		}
		sprintf(info.szName, "%s", szValue); 
		//
		if (pFile.GetItemString(szSection, "TunnelType", szValue))
		{
			sprintf(info.szType, "%s", szValue);
		}
		else
		{
			sprintf(info.szType, "ThingsDataLink");
		}
		//
		if (pFile.GetItemString(szSection, "TunnelDirect", szValue))
		{
			sprintf(info.szDirect, "%s", szValue);
		}
		else
		{
			sprintf(info.szDirect, "Both");
		}
		//
		if (pFile.GetItemString(szSection, "TunnelCode", szValue))
		{
			sprintf(info.szCode, "%s", szValue);
		}
		if (pFile.GetItemString(szSection, "TunnelReadParseCode", szValue))
		{
			info.szReadParseCode = szValue;
		}
		else
		{
			info.szReadParseCode = "";
		}
		if (pFile.GetItemString(szSection, "TunnelWriteParseCode", szValue))
		{
			info.szWriteParseCode = szValue;
		}
		else
		{
			info.szWriteParseCode = "";
		}
		//
		if (!pFile.GetItemString(szSection, "MasteHost", szValue))
		{
			pFile.CloseIniFile();
			return -1;
		}
		sprintf(info.szMasteHost, "%s", szValue);
		//
		if (!pFile.GetItemInt(szSection, "MastePort", nValue))
		{
			LogEvent("ReadMqttTunnel #MastePort");
			pFile.CloseIniFile();
			return -1;
		}
		info.nMastePort = nValue;
		//
		if (!pFile.GetItemString(szSection, "SlaveHost", szValue))
		{
			LogEvent("ReadMqttTunnel #SlaveHost");
			pFile.CloseIniFile();
			return -1;
		}
		sprintf(info.szSlaveHost, "%s", szValue);
		//
		if (!pFile.GetItemInt(szSection, "SlavePort", nValue))
		{
			LogEvent("ReadMqttTunnel #SlavePort");
			pFile.CloseIniFile();
			return -1;
		}
		info.nSlavePort = nValue;
		//
		if (pFile.GetItemString(szSection, "MqttHost", szValue))
		{
			sprintf(info.szMqttHost, "%s", szValue);
		}
		//
		if (pFile.GetItemInt(szSection, "MqttPort", nValue))
		{
			info.nMqttPort = nValue;
		}
		else
		{
			info.nMqttPort = 1883;
		}
		//
		if (pFile.GetItemString(szSection, "MqttFormat", szValue))
		{
			sprintf(info.szMqttFormat, "%s", szValue);
		}
		if (pFile.GetItemString(szSection, "MqttUserName", szValue))
		{
			sprintf(info.szMqttUserName, "%s", szValue);
		}
		if (pFile.GetItemString(szSection, "MqttPassWord", szValue))
		{
			sprintf(info.szMqttPassWord, "%s", szValue);
		}
		if (pFile.GetItemString(szSection, "MqttFilter", szValue))
		{
			sprintf(info.szMqttFilter, "%s", szValue);
		}
		else
		{
			sprintf(info.szMqttFilter, "#");
		}
		//
		if (pFile.GetItemString(szSection, "AmqpHost", szValue))
		{
			sprintf(info.szAmqpHost, "%s", szValue);
		}
		//
		if (pFile.GetItemInt(szSection, "AmqpPort", nValue))
		{
			info.nAmqpPort = nValue;
		}
		else
		{
			info.nAmqpPort = 5672;
		}
		//
		if (pFile.GetItemString(szSection, "AmqpUserName", szValue))
		{
			sprintf(info.szAmqpUserName, "%s", szValue);
		}
		//
		if (pFile.GetItemString(szSection, "AmqpPassWord", szValue))
		{
			sprintf(info.szAmqpPassWord, "%s", szValue);
		}
		//
		if (pFile.GetItemString(szSection, "AmqpExchange", szValue))
		{
			sprintf(info.szAmqpExchange, "%s", szValue);
		}
		//
		if (pFile.GetItemString(szSection, "AmqpQueue", szValue))
		{
			sprintf(info.szAmqpQueue, "%s", szValue);
		}
		//
		if (pFile.GetItemString(szSection, "AmqpFormat", szValue))
		{
			sprintf(info.szAmqpFormat, "%s", szValue);
		}
		//
		if (pFile.GetItemString(szSection, "KafkaBroker", szValue))
		{
			sprintf(info.szKafkaBroker, "%s", szValue);
		}
		//
		if (pFile.GetItemString(szSection, "KafkaUserName", szValue))
		{
			sprintf(info.szKafkaUserName, "%s", szValue);
		}
		//
		if (pFile.GetItemString(szSection, "KafkaPassWord", szValue))
		{
			sprintf(info.szKafkaPassWord, "%s", szValue);
		}
		//
		if (pFile.GetItemString(szSection, "ProducerTopic", szValue))
		{
			sprintf(info.szProducerTopic, "%s", szValue);
		}
		//
		if (pFile.GetItemString(szSection, "ConsumerTopic", szValue))
		{
			sprintf(info.szConsumerTopic, "%s", szValue);
		}
		//
		if (pFile.GetItemString(szSection, "KafkaFormat", szValue))
		{
			sprintf(info.szKafkaFormat, "%s", szValue);
		}
		//
		if (!pFile.GetItemString(szSection, "MqttSlaveHost", szValue))
		{
			LogEvent("ReadMqttTunnel #MqttSlaveHost");
			pFile.CloseIniFile();
			return -1;
		}
		sprintf(info.szMqttSlaveHost, "%s", szValue);
		//
		if (pFile.GetItemInt(szSection, "MqttSlavePort", nValue))
		{
			info.nMqttSlavePort = nValue;
		}
		else
		{
			info.nMqttSlavePort = 1883;
		}
		//
		if (pFile.GetItemString(szSection, "MqttSlaveFormat", szValue))
		{
			sprintf(info.szMqttSlaveFormat, "%s", szValue);
		}
		if (pFile.GetItemString(szSection, "MqttSlaveCode", szValue))
		{
			sprintf(info.szMqttSlaveCode, "%s", szValue);
		}
		if (pFile.GetItemString(szSection, "MqttSlaveUserName", szValue))
		{
			sprintf(info.szMqttSlaveUserName, "%s", szValue);
		}
		if (pFile.GetItemString(szSection, "MqttSlavePassWord", szValue))
		{
			sprintf(info.szMqttSlavePassWord, "%s", szValue);
		}
		if (pFile.GetItemString(szSection, "MqttReadParseCode", szValue))
		{
			info.szMqttReadParseCode = szValue;
		}
		else
		{
			info.szMqttReadParseCode = "";
		}
		if (pFile.GetItemString(szSection, "MqttWriteParseCode", szValue))
		{
			info.szMqttWriteParseCode = szValue;
		}
		else
		{
			info.szMqttWriteParseCode = "";
		}
		//
		if (!pFile.GetItemString(szSection, "MqttSlaveFilter", szValue))
		{
			LogEvent("ReadMqttTunnel #MqttSlaveFilter");
			pFile.CloseIniFile();
			return -1;
		}
		sprintf(info.szMqttSlaveFilter, "%s", szValue);
		//
		if (nTunnelID == 0)
		{
			BOOL bFindFlag = FALSE;
			for (DWORD j = 0; j < m_ProcessList.size(); j++)
			{
				if (m_ProcessList[j].nTunnelID == info.TunnelID)
				{
					bFindFlag = TRUE;
				}
			}
			//
			if (!bFindFlag)
			{
				struct ProcessParamItem process;
				memset(&process, 0, sizeof(process));
				strcpy(process.szTunnelName, info.szName);
				process.nTunnelID = info.TunnelID;
				//
				m_ProcessList.push_back(process);
			}
		}
		else if (info.TunnelID == nTunnelID)
		{
			info.hReadOPCMutex = CreateMutex(NULL, FALSE, NULL);
			m_ConfigList.push_back(info);
			//
			ReadMqttTable(info.TunnelID);
		}
	}
	pFile.CloseIniFile();
	return 0;
}

int CService::ReadMqttTable(DWORD TunnelID)
{
	char pModuleFile[8192];
	DWORD dwSize = GetModuleFileName(NULL, pModuleFile, 8192);
	pModuleFile[dwSize] = 0;
	if (dwSize>4 && pModuleFile[dwSize - 4] == '.')
	{
		pModuleFile[dwSize - 14] = 0;
	}
	//
	CStdioFile hCSVFile;
	char szFilePath[8192 + 1];
	sprintf(szFilePath, "%scsv\\MQ%lu.csv", pModuleFile, TunnelID);
	if (hCSVFile.Open(szFilePath, CFile::shareDenyNone))
	{
		int nRow = 0;
		CString szRowValue;
		//
		int idx = -1;
		for (DWORD i = 0; i < m_ConfigList.size(); i++)
		{
			if (m_ConfigList[i].TunnelID == TunnelID)
			{
				//m_ConfigList[i].hReadOPCMutex = CreateMutex(NULL, FALSE, NULL);
				idx = i;
				break;
			}
		}
		if (idx==-1)
		{
			hCSVFile.Close();
			return -1;
		}
		//
		while (hCSVFile.ReadString(szRowValue))
		{
			nRow++;
			//
			if (nRow>1 && szRowValue.GetLength()>0)
			{
				char line[8192];
				memset(line, 0, 8192);
				sprintf(line, "%s", szRowValue);
				//
				char buff[16][1024];
				int i = 0;
				for (i = 0; i<16; i++)
				{
					memset(buff[i], 0, 1024);
				}
				//
				char *p1;
				char *p2;
				i = 0;
				p1 = line;
				while (p1 != NULL)
				{
					p2 = strchr(p1, ',');
					if (p2 != NULL)
					{
						memcpy(buff[i++], p1, p2 - p1);
						p1 = p2 + 1;
					}
					else
					{
						strcpy(buff[i++], p1);
						break;
					}
				}
				//
				struct MqttToOPCValueItem info;
				memset(&info, 0, sizeof(MqttToOPCValueItem));
				info.id = atol(buff[0]);
				CString szColValue = buff[1];
				szColValue = szColValue.Trim();
				sprintf(info.no, "%s", szColValue);
				szColValue = buff[2];
				szColValue = szColValue.Trim();
				sprintf(info.szDstOPCPath, "%s", szColValue);
				szColValue = buff[3];
				szColValue = szColValue.Trim();
				sprintf(info.name, "%s", szColValue);
				szColValue = buff[4];
				szColValue = szColValue.Trim();
				sprintf(info.szSrcTopic, "%s", szColValue);
				info.fDiv = atof(buff[5]);
				info.fDlt = atof(buff[6]);
				info.fDead = atof(buff[7]);
				sprintf(info.szUnit, "%s", buff[8]);
				//
				if (stricmp(buff[9], "FALSE") == 0)
				{
					info.bMonitorValue = false;
				}
				else
				{
					info.bMonitorValue = true;
				}
				//
				sprintf(info.szDispType, "%s", buff[10]);
				info.nDispDiv = atol(buff[11]);
				//
				m_ConfigList[idx].list.push_back(info);
			}
		}
		hCSVFile.Close();
		return 0;
	}
	return -1;
}

