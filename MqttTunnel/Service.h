// Service.h: interface for the CService class.
//
//////////////////////////////////////////////////////////////////////

#if !defined(AFX_SERVICE_H__CCA2ED69_EC91_11D5_966E_000347A347FE__INCLUDED_)
#define AFX_SERVICE_H__CCA2ED69_EC91_11D5_966E_000347A347FE__INCLUDED_

#if _MSC_VER > 1000
#pragma once
#endif // _MSC_VER > 1000


class CService  
{
public:
	CService();
	virtual ~CService();

	void Init(LPCTSTR pServiceName,LPCTSTR pServiceDisplayedName);
    void Start(PCHAR	 pszTunnelName, DWORD nTunnelID);
	void ServiceMain();
    void Handler(DWORD dwOpcode);
    void Run(PCHAR	 pszTunnelName, DWORD nTunnelID);
    BOOL IsInstalled();
    BOOL Install();
    BOOL Uninstall(DWORD dwTimeout = 10000);
	

	CTime			m_UpdateTime;

	LPTSTR FormatMsg(DWORD dwError);

    void SetServiceStatus(DWORD dwState);
	//
	int ReadConfig(PCHAR	 pszTunnelName, DWORD nTunnelID);
	int ReadMqttTable(DWORD TunnelID);
	//
	int ReadTunnelStatus();
	int WriteTunnelStatus();
private:
	static void WINAPI _ServiceMain(DWORD dwArgc, LPTSTR* lpszArgv);
    static void WINAPI _Handler(DWORD dwOpcode);

	DWORD m_dwThreadID;

	char m_szFormatMsg[32];
	
	vector<struct MqttTunnelItem>		m_ConfigList;
	vector<struct ProcessParamItem>	m_ProcessList;
	vector<struct ProcessStatusItem>		m_StatusList;
public:
    TCHAR m_szServiceName[256];
	TCHAR m_szServiceDisplayedName[256];
    SERVICE_STATUS_HANDLE m_hServiceStatus;
    SERVICE_STATUS m_status;
	BOOL m_bService;
};

extern CService _Module;
#endif // !defined(AFX_SERVICE_H__CCA2ED69_EC91_11D5_966E_000347A347FE__INCLUDED_)
