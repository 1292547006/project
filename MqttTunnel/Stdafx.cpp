// stdafx.cpp : source file that includes just the standard includes
//	MouseRemoteControl.pch will be the pre-compiled header
//	stdafx.obj will contain the pre-compiled type information

#include "stdafx.h"
#include "Service.h"
//全局变量
extern volatile	int	g_DebugMode;
extern volatile	int	g_RunServrice;
extern char		g_ServricePath[8192];
extern char		g_DebugLogPath[8192];
extern volatile	int	g_OffineSpan;
extern volatile	int	g_OffineRecord;
extern volatile	int	g_ActiveTime;
extern CString	g_szMachineNO;
extern HANDLE g_hMqttWriteMutex;
extern BOOL g_bMqttConnect;
extern mosqpp::mosquittopp		*g_pMqttPub;
//
volatile int g_DebugCount=0;

void WriteCSVFile(DWORD TunnelID,BOOL bFirst, const char* fmt, ...)
{
	FILE *pf;

	va_list pArg;
	int len;
	char * chMsg;

	va_start(pArg, fmt);
	len = _vscprintf(fmt, pArg) // _vscprintf doesn't count
		+ 1; // terminating '\0'
	chMsg = (char *)malloc(len * sizeof(char));
	vsprintf_s(chMsg, len, fmt, pArg);
	va_end(pArg);
	chMsg[len - 1] = '\0';

	SYSTEMTIME st;
	GetLocalTime(&st);

	char  szCSVFile[8192];
	sprintf_s(szCSVFile, 8192, "%sappdata\\%lu.csv", g_ServricePath, TunnelID);
	if (bFirst)
		pf = fopen(szCSVFile, "w+t");
	else
		pf = fopen(szCSVFile, "a+t");

	if (pf)
	{
		fprintf(pf, "%s\n", chMsg);
		fclose(pf);

	}

	OutputDebugString(chMsg);

	free(chMsg);
	return;
}


LPTSTR FormatMsg(DWORD dwError)
{
	LPTSTR lpszMsg = NULL;
	if (!FormatMessage(FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_ALLOCATE_BUFFER,
		NULL,
		dwError,
		MAKELANGID(LANG_ENGLISH, SUBLANG_ENGLISH_US),
		(LPTSTR)&lpszMsg,
		0,
		NULL))
	{
		char szFormatMsgCode[32];
		memset(szFormatMsgCode, 0, 32);
		sprintf_s(szFormatMsgCode, "%u", dwError);
		return szFormatMsgCode;
	}
	return lpszMsg;
}

/********************************************************************/
/*																	*/
/* Function name : AfxHexValue										*/
/* Description   : Get the decimal value of a hexadecimal character	*/
/*																	*/
/********************************************************************/
short AfxHexValue(char chIn)
{
	unsigned char ch = (unsigned char)chIn;
	if (ch >= '0' && ch <= '9')
		return (short)(ch - '0');
	if (ch >= 'A' && ch <= 'F')
		return (short)(ch - 'A' + 10);
	if (ch >= 'a' && ch <= 'f')
		return (short)(ch - 'a' + 10);
	return -1;
}


/********************************************************************/
/*																	*/
/* Function name : AfxIsUnsafeUrlChar								*/
/* Description   : Determine if character is unsafe					*/
/*																	*/
/********************************************************************/
BOOL AfxIsUnsafeUrlChar(char chIn)
{
	unsigned char ch = (unsigned char)chIn;
	switch (ch)
	{
	case ';': case '\\': case '?': case '@': case '&':
	case '=': case '+': case '$': case ',': case ' ':
	case '<': case '>': case '#': case '%': case '\"':
	case '{': case '}': case '|':
	case '^': case '[': case ']': case '`':
		return TRUE;
	default:
	{
		if (ch < 32 || ch > 126)
			return TRUE;
		return FALSE;
	}
	}
}


CString URLEncode(LPCTSTR lpszURL)
{
	CString strResult = "";
	CString strHex;

	unsigned char ch;
	while ((ch = *lpszURL++) != '\0')
	{
		// check if it's an unsafe character
		if (AfxIsUnsafeUrlChar(ch))
		{
			if (ch == ' ')
				strResult += '+';
			else
			{
				// output the percent, followed by the hex value of the character
				strResult += '%';
				strHex.Format("%02X", ch);
				strResult += strHex;
			}
		}
		else
			// safe character
		{
			strResult += ch;
		}
	}
	return strResult;
}

CString escape(LPCTSTR lpszBody)
{
	int len = MultiByteToWideChar(CP_ACP, 0, (LPCTSTR)lpszBody, -1, NULL, 0);
	wchar_t * wszUtf8 = new wchar_t[len + 1];
	wchar_t * wszUtf8Back = wszUtf8;
	memset(wszUtf8, 0, len * 2 + 2);
	MultiByteToWideChar(CP_ACP, 0, (LPCTSTR)lpszBody, -1, wszUtf8, len);

	CString strResult = "";
	CString strHex;

	DWORD ch;
	while ((ch = *wszUtf8++) != 0)
	{
		if (ch <= 0x007F)
		{
			strResult += '%';
			strHex.Format("%02X", ch);
			strResult += strHex;
		}
		else
		{
			strResult += "%u";
			strHex.Format("%04X", ch);
			strResult += strHex;
		}
	}

	delete[] wszUtf8Back;

	return strResult;
}

//函数
int SendSocketData(SOCKET s, char *DataSendBuff,int len)
{
	int xs,xoff=0,reTry=0;
	do 
	{
		xs=send(s,(char *)DataSendBuff+xoff,len-xoff,0);
		if(xs== SOCKET_ERROR)
		{
			reTry++;
			if(reTry>50)
			{
				reTry=0;
				return -1;
			}
			Sleep(1);
		}
		else
		{
			xoff+=xs;
		}
	} while(xoff<len);

	return len;
}

int ReadSocketData(SOCKET s, char *DataRcvBuff, int len)
{
	int nCount=0;
	int xs,xoff=0;
	do{
		//===============================================
		fd_set fdread;
		int ret;
		struct timeval tv;
		FD_ZERO(&fdread);
		FD_SET(s,&fdread);
		tv.tv_sec=5;
		tv.tv_usec=0;
		ret=select(0,&fdread,NULL,NULL,&tv);
		if(ret<0)
		{
			return -1;
		}
		else if(ret==0)
		{
			nCount++;
			if(nCount>24)
			{
				return -1;
			}
		}
		else
		{
			if(FD_ISSET(s,&fdread))
			{
				xs=recv(s,(char *)DataRcvBuff+xoff,len-xoff,0);
				if(xs <=0)
				{
					return -1;
				}
				xoff+=xs;
			}
			else
			{
				return -1;
			}
		}
	}while(xoff<len);
	
	return len;
}

int SendSocketCMD(SOCKET s, u_int32_t CmdMsg, u_int32_t len)
{
	struct CmdHdr rp;
	rp.uCmd=CmdMsg;
	rp.uLen=len;
	rp.uMagic=NET_CMD_MAGIC;
	
	int xs=send(s,(char *)&rp,sizeof(rp),0);
	if(xs == SOCKET_ERROR )
	{
		return -1;
	}
	return len;
}
int ReadSocketCMD(SOCKET s, u_int32_t  *CmdMsg,u_int32_t  *uLen)
{
	int nCount=0;
	struct CmdHdr   head;
	int  xs,xoff=0;
	int  len=sizeof(head);
	do
	{
		//===============================================
		fd_set fdread;
		int ret;
		struct timeval tv;
		FD_ZERO(&fdread);
		FD_SET(s,&fdread);
		tv.tv_sec=5;
		tv.tv_usec=0;
		ret=select(0,&fdread,NULL,NULL,&tv);
		if(ret<0)
		{
			return -1;
		}
		else if(ret==0)
		{
			nCount++;
			if(nCount>24)
			{
				return -1;
			}
		}
		else
		{
			if(FD_ISSET(s,&fdread))
			{
				xs = recv(s,(char *)&head+xoff,len-xoff,0);
				if(xs <=0 )
				{
					return -1;
				}
				xoff += xs;
			}
			else
			{
				return -1;
			}
		}
	}while(xoff < len);
	
	if(head.uMagic==NET_CMD_MAGIC)
	{
		*CmdMsg=head.uCmd;
		*uLen=head.uLen;
		return len;
	}
	return -1;
}


int SendSocketRES(SOCKET s, u_int32_t uRes, u_int32_t len)
{
	struct ResHdr rp;
	rp.uRes=uRes;
	rp.uLen=len;
	rp.uMagic=NET_RES_MAGIC;
	
	int xs=send(s,(char *)&rp,sizeof(rp),0);
	if(xs == SOCKET_ERROR )
	{
		return -1;
	}
	return len;
}

int ReadSocketRES(SOCKET s, u_int32_t  *uRes,u_int32_t  *uLen)
{
	int nCount=0;
	struct ResHdr   head;
	int  xs,xoff=0;
	int  len=sizeof(head);
	do
	{
		//===============================================
		fd_set fdread;
		int ret;
		struct timeval tv;
		FD_ZERO(&fdread);
		FD_SET(s,&fdread);
		tv.tv_sec=5;
		tv.tv_usec=0;
		ret=select(0,&fdread,NULL,NULL,&tv);
		if(ret<0)
		{
			return -1;
		}
		else if(ret==0)
		{
			nCount++;
			if(nCount>24)
			{
				return -1;
			}
		}
		else
		{
			if(FD_ISSET(s,&fdread))
			{
				xs = recv(s,(char *)&head+xoff,len-xoff,0);
				if(xs <=0 )
				{
					return -1;
				}
				xoff += xs;
			}
			else
			{
				return -1;
			}
		}
	}while(xoff < len);
	
	if(head.uMagic==NET_RES_MAGIC)
	{
		*uRes=head.uRes;
		*uLen=head.uLen;
		return len;
	}
	return -1;
}



CString ConvertGBKToUtf16(CString strGBK) {
	int len = MultiByteToWideChar(CP_ACP, 0, (LPCTSTR)strGBK, -1, NULL, 0);
	wchar_t * wszUtf8 = new wchar_t[len + 1];
	memset(wszUtf8, 0, len * 2 + 2);
	MultiByteToWideChar(CP_ACP, 0, (LPCTSTR)strGBK, -1, wszUtf8, len);

	CString szTemp1 = _T("");
	szTemp1 = (char *)wszUtf8;
	delete[] wszUtf8;
	return szTemp1;
}


CString ConvertGBKToUtf8(CString strGBK) {
	int len = MultiByteToWideChar(CP_ACP, 0, (LPCTSTR)strGBK, -1, NULL, 0);
	wchar_t * wszUtf8 = new wchar_t[len + 1];
	memset(wszUtf8, 0, len * 2 + 2);
	MultiByteToWideChar(CP_ACP, 0, (LPCTSTR)strGBK, -1, wszUtf8, len);

	len = WideCharToMultiByte(CP_UTF8, 0, wszUtf8, -1, NULL, 0, NULL, NULL);
	char *szUtf8 = new char[len + 1];
	memset(szUtf8, 0, len + 1);
	WideCharToMultiByte(CP_UTF8, 0, wszUtf8, -1, szUtf8, len, NULL, NULL);

	CString szTemp1 = _T("");
	szTemp1 = szUtf8;
	delete[] szUtf8;
	delete[] wszUtf8;
	return szTemp1;
}

CString ConvertUtf8ToGBK(CString strUTF8) {
	int len = MultiByteToWideChar(CP_UTF8, 0, (LPCTSTR)strUTF8, -1, NULL, 0);
	wchar_t * wszUtf8 = new wchar_t[len + 1];
	memset(wszUtf8, 0, len * 2 + 2);
	MultiByteToWideChar(CP_UTF8, 0, (LPCTSTR)strUTF8, -1, wszUtf8, len);

	len = WideCharToMultiByte(CP_UTF8, 0, wszUtf8, -1, NULL, 0, NULL, NULL);
	char *szUtf8 = new char[len + 1];
	memset(szUtf8, 0, len + 1);
	WideCharToMultiByte(CP_ACP, 0, wszUtf8, -1, szUtf8, len, NULL, NULL);

	CString szTemp1 = _T("");
	szTemp1 = szUtf8;
	delete[] szUtf8;
	delete[] wszUtf8;
	return szTemp1;
}

CString ConvertUtf16ToGBK(CStringW strUTF16) {
	int len = WideCharToMultiByte(CP_OEMCP, 0, strUTF16, -1, NULL, 0, NULL, NULL);
	char *szUtf16 = new char[len + 1];
	memset(szUtf16, 0, len + 1);
	WideCharToMultiByte(CP_OEMCP, 0, strUTF16, -1, szUtf16, len, NULL, NULL);

	CString szTemp1 = _T("");
	szTemp1 = szUtf16;
	delete[] szUtf16;
	return szTemp1;
}

CString FormatValueType(double fValue, const char *szType, int nDispDiv)
{
	CString szTempVariant;
	if (stricmp(szType, "float") == 0)		//浮点
	{
		char szFormat[256];
		memset(szFormat, 0, 256);
		if (nDispDiv>0)
			sprintf(szFormat, "%%.%df", nDispDiv);
		else
			sprintf(szFormat, "%%f");
		szTempVariant.Format(szFormat, fValue);
	}
	else if (stricmp(szType, "int") == 0 || stricmp(szType, "long") == 0)		//浮点
	{
		szTempVariant.Format("%ld", (long)fValue);
	}
	else if (stricmp(szType, "bool") == 0)		// 布尔
	{
		if (fValue>0)
			szTempVariant.Format("1");
		else
			szTempVariant.Format("0");
	}
	else
	{
		szTempVariant.Format("%f", fValue);
	}
	return szTempVariant;
}


//功  能：在lpszSour中查找字符串lpszFind，lpszFind中可以包含通配字符‘?’
//参  数：nStart为在lpszSour中的起始查找位置
//返回值：成功返回匹配位置，否则返回-1
//注  意：Called by “bool MatchingString()”
int FindingString(const char* lpszSour, const char* lpszFind, int nStart /* = 0 */)
{
//	ASSERT(lpszSour && lpszFind && nStart >= 0);
	if(lpszSour == NULL || lpszFind == NULL || nStart < 0)
		return -1;

	int m = strlen(lpszSour);
	int n = strlen(lpszFind);

	if( nStart+n > m )
		return -1;

	if(n == 0)
		return nStart;

//KMP算法
	int* next = new int[n];
	//得到查找字符串的next数组
	{	n--;

		int j, k;
		j = 0;
		k = -1;
		next[0] = -1;

		while(j < n)
		{	if(k == -1 || lpszFind[k] == '?' || lpszFind[j] == lpszFind[k])
			{	j++;
				k++;
				next[j] = k;
			}
			else
				k = next[k];
		}

		n++;
	}

	int i = nStart, j = 0;
	while(i < m && j < n)
	{
		if(j == -1 || lpszFind[j] == '?' || lpszSour[i] == lpszFind[j])
		{	i++;
			j++;
		}
		else
			j = next[j];
	}

	delete []next;

	if(j >= n)
		return i-n;
	else
		return -1;
}

//功	  能：带通配符的字符串匹配
//参	  数：lpszSour是一个普通字符串；
//			  lpszMatch是一可以包含通配符的字符串；
//			  bMatchCase为0，不区分大小写，否则区分大小写。
//返  回  值：匹配，返回1；否则返回0。
//通配符意义：
//		‘*’	代表任意字符串，包括空字符串；
//		‘?’	代表任意一个字符，不能为空；
//时	  间：	2001.11.02	13:00
bool MatchingString(const char* lpszSour, const char* lpszMatch, bool bMatchCase /*  = true */)
{
//	ASSERT(AfxIsValidString(lpszSour) && AfxIsValidString(lpszMatch));
	if(lpszSour == NULL || lpszMatch == NULL)
		return false;

	if(lpszMatch[0] == 0)//Is a empty string
	{
		if(lpszSour[0] == 0)
			return true;
		else
			return false;
	}

	int i = 0, j = 0;

	//生成比较用临时源字符串'szSource'
	char* szSource = new char[ (j = strlen(lpszSour)+1) ];

	if( bMatchCase )
	{	//memcpy(szSource, lpszSour, j);
		while( *(szSource+i) = *(lpszSour+i++) );
	}
	else
	{	//Lowercase 'lpszSour' to 'szSource'
		i = 0;
		while(lpszSour[i])
		{	if(lpszSour[i] >= 'A' && lpszSour[i] <= 'Z')
				szSource[i] = lpszSour[i] - 'A' + 'a';
			else
				szSource[i] = lpszSour[i];

			i++;
		}
		szSource[i] = 0;
	}

	//生成比较用临时匹配字符串'szMatcher'
	char* szMatcher = new char[strlen(lpszMatch)+1];

	//把lpszMatch里面连续的“*”并成一个“*”后复制到szMatcher中
	i = j = 0;
	while(lpszMatch[i])
	{
		szMatcher[j++] = (!bMatchCase) ?
								( (lpszMatch[i] >= 'A' && lpszMatch[i] <= 'Z') ?//Lowercase lpszMatch[i] to szMatcher[j]
										lpszMatch[i] - 'A' + 'a' :
										lpszMatch[i]
								) :
								lpszMatch[i];		 //Copy lpszMatch[i] to szMatcher[j]
		//Merge '*'
		if(lpszMatch[i] == '*')
			while(lpszMatch[++i] == '*');
		else
			i++;
	}
	szMatcher[j] = 0;

	//开始进行匹配检查

	int nMatchOffset, nSourOffset;

	bool bIsMatched = true;
	nMatchOffset = nSourOffset = 0;
	while(szMatcher[nMatchOffset])
	{
		if(szMatcher[nMatchOffset] == '*')
		{
			if(szMatcher[nMatchOffset+1] == 0)
			{	//szMatcher[nMatchOffset]是最后一个字符

				bIsMatched = true;
				break;
			}
			else
			{	//szMatcher[nMatchOffset+1]只能是'?'或普通字符

				int nSubOffset = nMatchOffset+1;

				while(szMatcher[nSubOffset])
				{	if(szMatcher[nSubOffset] == '*')
						break;
					nSubOffset++;
				}

				if( strlen(szSource+nSourOffset) <
						size_t(nSubOffset-nMatchOffset-1) )
				{	//源字符串剩下的长度小于匹配串剩下要求长度
					bIsMatched = false; //判定不匹配
					break;			//退出
				}

				if(!szMatcher[nSubOffset])//nSubOffset is point to ender of 'szMatcher'
				{	//检查剩下部分字符是否一一匹配

					nSubOffset--;
					int nTempSourOffset = strlen(szSource)-1;
					//从后向前进行匹配
					while(szMatcher[nSubOffset] != '*')
					{
						if(szMatcher[nSubOffset] == '?')
							;
						else
						{	if(szMatcher[nSubOffset] != szSource[nTempSourOffset])
							{	bIsMatched = false;
								break;
							}
						}
						nSubOffset--;
						nTempSourOffset--;
					}
					break;
				}
				else//szMatcher[nSubOffset] == '*'
				{	nSubOffset -= nMatchOffset;

					char* szTempFinder = new char[nSubOffset];
					nSubOffset--;
					memcpy(szTempFinder, szMatcher+nMatchOffset+1, nSubOffset);
					szTempFinder[nSubOffset] = 0;

					int nPos = ::FindingString(szSource+nSourOffset, szTempFinder, 0);
					delete []szTempFinder;

					if(nPos != -1)//在'szSource+nSourOffset'中找到szTempFinder
					{	nMatchOffset += nSubOffset;
						nSourOffset += (nPos+nSubOffset-1);
					}
					else
					{	bIsMatched = false;
						break;
					}
				}
			}
		}		//end of "if(szMatcher[nMatchOffset] == '*')"
		else if(szMatcher[nMatchOffset] == '?')
		{
			if(!szSource[nSourOffset])
			{	bIsMatched = false;
				break;
			}
			if(!szMatcher[nMatchOffset+1] && szSource[nSourOffset+1])
			{	//如果szMatcher[nMatchOffset]是最后一个字符，
				//且szSource[nSourOffset]不是最后一个字符
				bIsMatched = false;
				break;
			}
			nMatchOffset++;
			nSourOffset++;
		}
		else//szMatcher[nMatchOffset]为常规字符
		{
			if(szSource[nSourOffset] != szMatcher[nMatchOffset])
			{	bIsMatched = false;
				break;
			}
			if(!szMatcher[nMatchOffset+1] && szSource[nSourOffset+1])
			{	bIsMatched = false;
				break;
			}
			nMatchOffset++;
			nSourOffset++;
		}
	}

	delete []szSource;
	delete []szMatcher;
	return bIsMatched;
}

//功  能：多重匹配，不同匹配字符串之间用‘,’隔开
//			如：“*.h,*.cpp”将依次匹配“*.h”和“*.cpp”
//参  数：nMatchLogic = 0, 不同匹配求或，else求与；bMatchCase, 是否大小敏感
//返回值：如果bRetReversed = 0, 匹配返回true；否则不匹配返回true
//时  间：2001.11.02  17:00
bool MultiMatching(const char* lpszSour, const char* lpszMatch, int nMatchLogic /* = 0 */, bool bRetReversed /* = 0 */, bool bMatchCase /* = true */)
{
//	ASSERT(AfxIsValidString(lpszSour) && AfxIsValidString(lpszMatch));
	if(lpszSour == NULL || lpszMatch == NULL)
		return false;

	char* szSubMatch = new char[strlen(lpszMatch)+1];
	bool bIsMatch;

	if(nMatchLogic == 0)//求或
	{	bIsMatch = 0;
		int i = 0;
		int j = 0;
		while(1)
		{	if(lpszMatch[i] != 0 && lpszMatch[i] != ',')
				szSubMatch[j++] = lpszMatch[i];
			else
			{	szSubMatch[j] = 0;
				if(j != 0)
				{
					bIsMatch = MatchingString(lpszSour, szSubMatch, bMatchCase);
					if(bIsMatch)
						break;
				}
				j = 0;
			}

			if(lpszMatch[i] == 0)
				break;
			i++;
		}
	}
	else//求与
	{	bIsMatch = 1;
		int i = 0;
		int j = 0;
		while(1)
		{	if(lpszMatch[i] != 0 && lpszMatch[i] != ',')
				szSubMatch[j++] = lpszMatch[i];
			else
			{	szSubMatch[j] = 0;

				bIsMatch = MatchingString(lpszSour, szSubMatch, bMatchCase);
				if(!bIsMatch)
					break;

				j = 0;
			}

			if(lpszMatch[i] == 0)
				break;
			i++;
		}
	}

	delete []szSubMatch;

	if(bRetReversed)
		return !bIsMatch;
	else
		return bIsMatch;
}

//
#if _MSC_VER >= 1100
template<> UINT AFXAPI HashKey<LPCSTR> (LPCSTR key)
#else
UINT AFXAPI HashKey(LPCSTR key)
#endif
{
	UINT nHash = 0;
	while (*key)
		nHash = (nHash << 5) + nHash + *key++;
	return nHash;
}


///////////////////////////////////////////////////////////////////////////////////////
// Logging functions
//
void LogEvent(LPCSTR pFormat, ...)
{
	if(g_DebugMode==0)
		return;
	//
    va_list pArg;
	char * chMsg;

    va_start(pArg, pFormat);
	long len = _vscprintf(pFormat, pArg) // _vscprintf doesn't count
		+ 1; // terminating '\0'
	if (len <= 1)
		return;
	chMsg = (char *)malloc(len * sizeof(char));
	if (chMsg == NULL)
		return;

	vsprintf_s(chMsg, len, pFormat, pArg);
    va_end(pArg);
	chMsg[len - 1] = '\0';

	if (!g_bMqttConnect) {
		OutputDebugString(chMsg);
		//
		int keepalive = 60;
		if (g_pMqttPub->connect("localhost", 1883, keepalive) == 0)
			g_bMqttConnect = TRUE;
		else
			g_bMqttConnect = FALSE;
	}
	else
	{
		SYSTEMTIME st;
		GetLocalTime(&st);
		CString pathMqtt = "";
		CString valDataMqtt = "";
		if (strstr(chMsg, "成功"))
		{
			pathMqtt = "DebugView/ThingsMix/MQTT/info";
			valDataMqtt.Format("{\"time\":\"%02d时%02d分%02d.%03d秒\",\"level\":\"%s\",\"user\":\"%s\",\"src\":\"%s\",\"msg\":\"%s\"}", st.wHour, st.wMinute, st.wSecond, st.wMilliseconds, "info", "MQTT", "系统服务", escape(chMsg));
		}
		else if (strstr(chMsg, "失败") || strstr(chMsg, "无法") || strstr(chMsg, "无效"))
		{
			pathMqtt = "DebugView/ThingsMix/MQTT/error";
			valDataMqtt.Format("{\"time\":\"%02d时%02d分%02d.%03d秒\",\"level\":\"%s\",\"user\":\"%s\",\"src\":\"%s\",\"msg\":\"%s\"}", st.wHour, st.wMinute, st.wSecond, st.wMilliseconds, "error", "MQTT", "系统服务", escape(chMsg));
		}
		else if (strstr(chMsg, "Error") || strstr(chMsg, "error") || strstr(chMsg, "failed"))
		{
			pathMqtt = "DebugView/ThingsMix/MQTT/fatal";
			valDataMqtt.Format("{\"time\":\"%02d时%02d分%02d.%03d秒\",\"level\":\"%s\",\"user\":\"%s\",\"src\":\"%s\",\"msg\":\"%s\"}", st.wHour, st.wMinute, st.wSecond, st.wMilliseconds, "fatal", "MQTT", "系统服务", escape(chMsg));
		}
		else
		{
			pathMqtt = "DebugView/ThingsMix/MQTT/warn";
			valDataMqtt.Format("{\"time\":\"%02d时%02d分%02d.%03d秒\",\"level\":\"%s\",\"user\":\"%s\",\"src\":\"%s\",\"msg\":\"%s\"}", st.wHour, st.wMinute, st.wSecond, st.wMilliseconds, "warn", "MQTT", "系统服务", escape(chMsg));
		}
		//
		valDataMqtt = ConvertGBKToUtf8(valDataMqtt);
		//
		WaitForSingleObject(g_hMqttWriteMutex, INFINITE);
		int ret = g_pMqttPub->publish(NULL, pathMqtt, valDataMqtt.GetLength(), valDataMqtt);
		if (ret != MOSQ_ERR_SUCCESS)
		{
			g_pMqttPub->reconnect();
		}
		ReleaseMutex(g_hMqttWriteMutex);
	}
	free(chMsg);
}

ProducerKafka::ProducerKafka()
{

}

ProducerKafka::~ProducerKafka()
{
	destroy();
}

int ProducerKafka::init_kafka(int partition, char *brokers, char *username, char *password, char *topic)
{
	char tmp[16] = { 0 };
	char errstr[512] = { 0 };

	partition_ = partition;

	/* Kafka configuration */
	conf_ = rd_kafka_conf_new();

	//set logger :register log function    
	rd_kafka_conf_set_log_cb(conf_, logger);

	/* Quick termination */
	if (strlen(username) > 0 && strlen(password) > 0)
	{
		rd_kafka_conf_set(conf_, "security.protocol", "SASL_PLAINTEXT", NULL, 0);
		rd_kafka_conf_set(conf_, "sasl.mechanism", "PLAIN", NULL, 0);
		rd_kafka_conf_set(conf_, "sasl.username", username, NULL, 0);
		rd_kafka_conf_set(conf_, "sasl.password", password, NULL, 0);
	}


	/*创建broker集群*/
	if (rd_kafka_conf_set(conf_, "bootstrap.servers", brokers, errstr,
		sizeof(errstr)) != RD_KAFKA_CONF_OK) {
		LogEvent("%s\n", errstr);
		return 1;
	}

	/*设置发送报告回调函数，rd_kafka_produce()接收的每条消息都会调用一次该回调函数
	*应用程序需要定期调用rd_kafka_poll()来服务排队的发送报告回调函数*/
	rd_kafka_conf_set_dr_msg_cb(conf_, dr_msg_cb);

	/*创建producer实例
	rd_kafka_new()获取conf对象的所有权,应用程序在此调用之后不得再次引用它*/
	handler_ = rd_kafka_new(RD_KAFKA_PRODUCER, conf_, errstr, sizeof(errstr));
	if (!handler_) {
		LogEvent("%% Failed to create new producer:%s\n", errstr);
		return 1;
	}
	rd_kafka_set_log_level(handler_, 7);
	/* Create topic */
	topic_ = rd_kafka_topic_new(handler_, topic, NULL);
	if (!topic_) {
		LogEvent("%% Failed to create topic object: %s\n",
			rd_kafka_err2str(rd_kafka_last_error()));
		rd_kafka_destroy(handler_);
		return 1;
	}
	return PRODUCER_INIT_SUCCESS;
}


int ProducerKafka::push_data_to_kafka(const char* buffer, const int buf_len, const char* key, const int key_len)
{
	int ret;
	char errstr[512] = { 0 };

	if (NULL == buffer)
		return 0;

	//
	LogEvent("push_data_to_kafka %s:%s", key, buffer);
	//

	if (stricmp(pMqttTunnelItem->szCode, "UTF8") == 0)
	{
		CString strDataUtf8 = ConvertGBKToUtf8(buffer);
		CString strKeyUtf8 = ConvertGBKToUtf8(key);
		ret = rd_kafka_produce(topic_, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
			(void*)strDataUtf8.GetBuffer(), (size_t)strDataUtf8.GetLength(), (void*)strKeyUtf8.GetBuffer(), (size_t)strKeyUtf8.GetLength(), NULL);
		strDataUtf8.ReleaseBuffer();
		strKeyUtf8.ReleaseBuffer();
	}
	else if (stricmp(pMqttTunnelItem->szCode, "Unicode") == 0)
	{
		CString strDataUnicode = ConvertGBKToUtf16(buffer);
		CString strKeyUnicode = ConvertGBKToUtf16(key);
		ret = rd_kafka_produce(topic_, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
			(void*)strDataUnicode.GetBuffer(), (size_t)strDataUnicode.GetLength(), (void*)strKeyUnicode.GetBuffer(), (size_t)strKeyUnicode.GetLength(), NULL);
		strDataUnicode.ReleaseBuffer();
		strKeyUnicode.ReleaseBuffer();
	}
	else
	{
		ret = rd_kafka_produce(topic_, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY,
			(void*)buffer, (size_t)buf_len, (void*)key, (size_t)key_len, NULL);
	}

	if (ret == -1)
	{
		int nunm = rd_kafka_poll(handler_, 1000);
		LogEvent("****Failed to produce to topic %s partition %i num %d: %s*****",
			rd_kafka_topic_name(topic_), partition_,
			rd_kafka_err2str(rd_kafka_last_error()));

		return PUSH_DATA_FAILED;
	}

	int nunm = rd_kafka_poll(handler_, 0);
	return PUSH_DATA_SUCCESS;
}

void ProducerKafka::destroy()
{
	rd_kafka_flush(handler_, 10 * 1000);
	/* Destroy topic */
	rd_kafka_topic_destroy(topic_);

	/* Destroy the handle */
	rd_kafka_destroy(handler_);
}



ConsummerKafka::ConsummerKafka()
{
	m_bConnectFlag = 0;

	m_MapTopicValue.InitHashTable(10001);
	m_MapPayload.InitHashTable(10001);
	m_MapTopicSrc.InitHashTable(10001);
	m_MapTopicDst.InitHashTable(10001);
}

ConsummerKafka::~ConsummerKafka()
{
	destroy();
}

int ConsummerKafka::init_kafka(char *brokers, char *username, char *password, char *group, char *topic)
{
	char tmp[16] = { 0 };
	char errstr[512] = { 0 };
	conf = rd_kafka_conf_new();

	if (strlen(username) > 0 && strlen(password) > 0)
	{
		rd_kafka_conf_set(conf, "security.protocol", "SASL_PLAINTEXT", NULL, 0);
		rd_kafka_conf_set(conf, "sasl.mechanism", "PLAIN", NULL, 0);
		rd_kafka_conf_set(conf, "sasl.username", username, NULL, 0);
		rd_kafka_conf_set(conf, "sasl.password", password, NULL, 0);
	}

	//topic configuration  
	topic_conf = rd_kafka_topic_conf_new();

	/* Consumer groups require a group id */
	if (!group)
		group = "rdkafka_consumer_iot";
	if (rd_kafka_conf_set(conf, "group.id", group,
		errstr, sizeof(errstr)) !=
		RD_KAFKA_CONF_OK) {
		LogEvent("%% %s\n", errstr);
		return -1;
	}

	/* Consumer groups always use broker based offset storage */
	if (rd_kafka_topic_conf_set(topic_conf, "offset.store.method",
		"broker",
		errstr, sizeof(errstr)) !=
		RD_KAFKA_CONF_OK) {
		LogEvent("%% %s\n", errstr);
		return -1;
	}

	/* Set default topic config for pattern-matched topics. */
	rd_kafka_conf_set_default_topic_conf(conf, topic_conf);

	//实例化一个顶级对象rd_kafka_t作为基础容器，提供全局配置和共享状态  
	rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
	if (!rk) {
		LogEvent("%% Failed to create new consumer:%s\n", errstr);
		return -1;
	}

	//Librdkafka需要至少一个brokers的初始化list  
	if (rd_kafka_brokers_add(rk, brokers) == 0) {
		LogEvent("%% No valid brokers specified\n");
		return -1;
	}

	//重定向 rd_kafka_poll()队列到consumer_poll()队列  
	rd_kafka_poll_set_consumer(rk);

	//创建一个Topic+Partition的存储空间(list/vector)  
	topics = rd_kafka_topic_partition_list_new(1);
	//把Topic+Partition加入list  
	rd_kafka_topic_partition_list_add(topics, topic, -1);
	//开启consumer订阅，匹配的topic将被添加到订阅列表中  
	if ((err = rd_kafka_subscribe(rk, topics))) {
		LogEvent("%% Failed to start consuming topics: %s\n", rd_kafka_err2str(err));
		return -1;
	}
	return 0;
}


int ConsummerKafka::msg_consume(rd_kafka_message_t *rkmessage,
	void *opaque) {
	if (rkmessage->err) {
		if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			LogEvent("%% Consumer reached end of %s [%lu] "
				"message queue at offset %lu\n",
				rd_kafka_topic_name(rkmessage->rkt),
				rkmessage->partition, rkmessage->offset);

			return -1;
		}

		if (rkmessage->rkt)
			LogEvent("%% Consume error for "
				"topic \"%s\" [%lu] "
				"offset %lu: %s\n",
				rd_kafka_topic_name(rkmessage->rkt),
				rkmessage->partition,
				rkmessage->offset,
				rd_kafka_message_errstr(rkmessage));
		else
			LogEvent("%% Consumer error: %s: %s\n",
				rd_kafka_err2str(rkmessage->err),
				rd_kafka_message_errstr(rkmessage));

		if (rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION ||
			rkmessage->err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC)
			run = 0;
		return -1;
	}

	if (rkmessage->key_len>0) {
		LogEvent("Key【%d】: %s\n",
			(int)rkmessage->key_len, (char *)rkmessage->key);
	}

	if (rkmessage->len>0)
	{
		CString strDataGBK = _T("");
		char * pKey = (char *)rkmessage->key;
		if (stricmp(pMqttTunnelItem->szCode, "UTF8") == 0)
		{
			strDataGBK = ConvertUtf8ToGBK((char *)(rkmessage->payload));
		}
		else if (stricmp(pMqttTunnelItem->szCode, "Unicode") == 0)
		{
			strDataGBK = ConvertUtf16ToGBK((char *)(rkmessage->payload));
		}
		else
		{
			strDataGBK = (char *)(rkmessage->payload);
		}

		DWORD uTime;
		double dblValue = 0;
		CTime t2 = CTime::GetCurrentTime();
		struct MqttToOPCValueItem	*pItem = NULL;
		//
		if (_stricmp(pMqttTunnelItem->szKafkaFormat, "TVU") == 0)
		{
			if (m_MqttMapTable1.Lookup(pKey, pItem) == 0)
			{
				return 0;
			}
			//
			if (pItem->bUpdateFlag > 0)
			{
				pItem->bUpdateFlag--;
				return 0;
			}
			//
			CString valFormat = "%19s|%lf|U";
			TCHAR szValueTime[32];
			int ret = sscanf_s(strDataGBK, valFormat, szValueTime, 32, &dblValue);
			if (ret == -1)
			{
				LogEvent("上行MQTT讀取变量（%s）失败，数据格式不正确(%s)！", pKey, valFormat);
				return 0;
			}
			szValueTime[10] = ' ';
			//
			COleDateTime t1;
			if (!t1.ParseDateTime(szValueTime))
			{
				LogEvent("上行MQTT讀取变量（%s）失败，时间格式不正确(%s)！", pKey, szValueTime);
				return 0;
			}
			t2 = CTime(t1.GetYear(), t1.GetMonth(), t1.GetDay(), t1.GetHour(), t1.GetMinute(), t1.GetSecond());
			m_MapTopicDst.SetAt(pItem->szDstOPCPath, pKey);
			//
			SYSTEMTIME  lt, st;
			TIME_ZONE_INFORMATION zinfo;
			GetTimeZoneInformation(&zinfo);

			t2.GetAsSystemTime(lt);
			::TzSpecificLocalTimeToSystemTime(&zinfo, &lt, &st);
			::SystemTimeToFileTime(&st, &(pItem->dataTime));
			//
			pItem->dblValue = dblValue;
			pItem->quality = OPC_QUALITY_GOOD;
			pItem->bUpdateFlag = 0;
			if (stricmp(pMqttTunnelItem->szType, "ThingsDataLink") == 0)
			{
				if (strlen(pMqttTunnelItem->szMasteHost) > 0)
				{
					pItem->bUpdateFlag++;
				}
				if (strlen(pMqttTunnelItem->szSlaveHost) > 0)
				{
					pItem->bUpdateFlag++;
				}
			}
			else
			{
				pItem->bUpdateFlag++;
			}
			//
			CMqttTunnel *pMqttTunnel = (CMqttTunnel *)m_pMqttHandle;
			struct TunnelValueItem info1;
			memset(&info1, 0, sizeof(struct TunnelValueItem));
			info1.id = pItem->id;
			strcpy(info1.no, pItem->no);
			strcpy(info1.szOPCPath, pItem->szDstOPCPath);
			info1.dblValue = pItem->dblValue;
			info1.TimeStamp = pItem->dataTime;
			info1.ValueType = VT_R8;
			if (pMqttTunnel->WriteItem(&info1) == 0)
			{
				LogEvent("写入本地变量成功（%s=%f).....", pItem->szDstOPCPath, dblValue);
			}
			else
			{

				LogEvent("写入本地变量失敗（%s=%f).....", pItem->szDstOPCPath, dblValue);
			}
		}
		else
		{
			CString szKey, szPayload;
			if (stricmp(pMqttTunnelItem->szMqttFormat, "BIN") == 0)
			{
				szPayload = "[";
				char *pData = strDataGBK.GetBuffer();
				for (int i = 0; i < strlen(pData); i++)
				{
					CString szTemp;
					szTemp.Format("%d,", (byte)pData[i]);
					szPayload += szTemp;
				}
				//
				strDataGBK.ReleaseBuffer();
				if (szPayload.GetLength()>1)
					szPayload = szPayload.Mid(0, szPayload.GetLength() - 1);
				szPayload += "]";
			}
			else
			{
				szPayload = strDataGBK;
			}
			m_MapPayload.SetAt(pKey, szPayload);
			//
			LogEvent("【北向来报】topic=%s，payload=%s", pKey, szPayload);
			//
			CString szParseResult = "";
			CMqttTunnel *pMqttTunnel = (CMqttTunnel *)m_pMqttHandle;
			if (pMqttTunnel->ParseMqttPayload(pMqttTunnelItem->szReadParseCode, pKey, pMqttTunnelItem->szMqttFormat, szPayload, szPayload.GetLength(), szParseResult) == -1)
			{
				LogEvent("北向MQTT讀取变量（%s）失败，数据解析不正确或返回为空！", pKey);
				return 0;
			}
			//
			LogEvent("【北向解析】topic=%s，payload=%s", pKey, szParseResult);
			//
			CString resToken;
			int curPos = 0;
			resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
			while (resToken != _T(""))
			{
				resToken.TrimLeft();
				if (resToken == _T(""))
				{
					resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
					continue;
				}
				//
				CString szTopicTag, szValueTime, szValueData;
				szTopicTag = resToken.Mid(0, resToken.Find('|'));
				resToken = resToken.Mid(resToken.Find('|') + 1);
				szValueTime = resToken.Mid(0, resToken.Find('|'));
				szValueTime.Replace("T", " ");
				szValueData = resToken.Mid(resToken.Find('|') + 1);
				//
				COleDateTime tc;
				if (!tc.ParseDateTime(szValueTime))
				{
					LogEvent("北向MQTT讀取变量（%s）失败，时间格式不正确(%s)！", pKey, resToken);
					return 0;
				}
				CTime t2 = CTime(tc.GetYear(), tc.GetMonth(), tc.GetDay(), tc.GetHour(), tc.GetMinute(), tc.GetSecond());
				//
				struct MqttToOPCValueItem	*pItem = NULL;
				if (m_MqttMapTable1.Lookup(szTopicTag, pItem) == 0)
				{
					resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
					continue;
				}
				m_MapTopicDst.SetAt(pItem->szDstOPCPath, pKey);
				m_MapPayload.SetAt(szTopicTag, szPayload);
				//
				LogEvent("【北向变量】%s—>%s = %f %s", pItem->szDstOPCPath, szTopicTag, dblValue, pItem->szUnit);
				//
				SYSTEMTIME  lt, st;
				TIME_ZONE_INFORMATION zinfo;
				GetTimeZoneInformation(&zinfo);

				t2.GetAsSystemTime(lt);
				::TzSpecificLocalTimeToSystemTime(&zinfo, &lt, &st);
				::SystemTimeToFileTime(&st, &(pItem->dataTime));
				//
				pItem->dblValue = dblValue;
				pItem->quality = OPC_QUALITY_GOOD;
				pItem->bUpdateFlag = 0;
				if (stricmp(pMqttTunnelItem->szType, "ThingsDataLink") == 0)
				{
					if (strlen(pMqttTunnelItem->szMasteHost) > 0)
					{
						pItem->bUpdateFlag++;
					}
					if (strlen(pMqttTunnelItem->szSlaveHost) > 0)
					{
						pItem->bUpdateFlag++;
					}
				}
				else
				{
					pItem->bUpdateFlag++;
				}
				//
				CMqttTunnel *pMqttTunnel = (CMqttTunnel *)m_pMqttHandle;
				struct TunnelValueItem info1;
				memset(&info1, 0, sizeof(struct TunnelValueItem));
				info1.id = pItem->id;
				strcpy(info1.no, pItem->no);
				strcpy(info1.szOPCPath, pItem->szDstOPCPath);
				info1.dblValue = pItem->dblValue;
				info1.TimeStamp = pItem->dataTime;
				info1.ValueType = VT_R8;
				if (pMqttTunnel->WriteItem(&info1) == 0)
				{
					LogEvent("写入本地变量成功（%s=%f).....", pItem->szDstOPCPath, dblValue);
				}
				else
				{

					LogEvent("写入本地变量失敗（%s=%f).....", pItem->szDstOPCPath, dblValue);
				}
				//
				resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
			}
		}
	}
}

int ConsummerKafka::pull_data_from_kafka()
{
	rd_kafka_message_t *rkmessage;
	rkmessage = rd_kafka_consumer_poll(rk, 1000);
	if (rkmessage) {
		msg_consume(rkmessage, NULL);
		/*释放rkmessage的资源，并把所有权还给rdkafka*/
		rd_kafka_message_destroy(rkmessage);
		return 0;
	}
	return -1;
}

void ConsummerKafka::destroy()
{
	rd_kafka_consumer_close(rk);
	rd_kafka_topic_partition_list_destroy(topics);
	rd_kafka_destroy(rk);

	while (g_RunServrice == 1 && rd_kafka_wait_destroyed(1000) == -1) {
		LogEvent("Waiting for librdkafka to decommission\n");
	}
}


CAmqpSmart::CAmqpSmart(string HostName, uint32_t port, string usr, string psw)
{
	m_pMqttHandle = NULL;
	m_bConnectFlag = 0;

	m_MapTopicValue.InitHashTable(10001);
	m_MapPayload.InitHashTable(10001);
	m_MapTopicSrc.InitHashTable(10001);
	m_MapTopicDst.InitHashTable(10001);

	m_MqttMapTable1.InitHashTable(10001);
	m_MqttMapTable2.InitHashTable(10001);
};

CAmqpSmart::~CAmqpSmart()
{
	Disconnect();
}

int	CAmqpSmart::subscribe()
{
	string err;
	string queue_name(pMqttTunnelItem->szAmqpQueue);
	vector<CMessage> vgetmsg;
	::timeval tvb = { 5,0 };
	vgetmsg.clear();
	CQueue queue_temp(queue_name);
	if (consumer(queue_temp, vgetmsg, 1, &tvb, err)<0)
	{
		LogEvent("订阅AMQP消息失败，%s", err.c_str());
		return -1;
	}
	//
	if (vgetmsg.size()>0)
	{
		DWORD uTime;
		double dblValue = 0;
		CTime t2 = CTime::GetCurrentTime();
		struct MqttToOPCValueItem	*pItem = NULL;
		//
		CString strDataGBK = _T("");
		const char * pKey = vgetmsg[0].m_routekey.c_str();
		if (stricmp(pMqttTunnelItem->szCode, "UTF8") == 0)
		{
			strDataGBK = ConvertUtf8ToGBK(vgetmsg[0].m_data.c_str());
		}
		else if (stricmp(pMqttTunnelItem->szCode, "Unicode") == 0)
		{
			strDataGBK = ConvertUtf16ToGBK(vgetmsg[0].m_data.c_str());
		}
		else
		{
			strDataGBK = vgetmsg[0].m_data.c_str();
		}
		//
		if (_stricmp(pMqttTunnelItem->szMqttFormat, "TVU") == 0)
		{
			if (m_MqttMapTable1.Lookup(pKey, pItem) == 0)
			{
				return 0;
			}
			//
			if (pItem->bUpdateFlag > 0)
			{
				pItem->bUpdateFlag--;
				return 0;
			}
			//
			CString valFormat = "%19s|%lf|U";
			TCHAR szValueTime[32];
			int ret = sscanf_s(strDataGBK, valFormat, szValueTime, 32, &dblValue);
			if (ret == -1)
			{
				LogEvent("上行MQTT讀取变量（%s）失败，数据格式不正确(%s)！", pKey, valFormat);
				return 0;
			}
			szValueTime[10] = ' ';
			//
			COleDateTime t1;
			if (!t1.ParseDateTime(szValueTime))
			{
				LogEvent("上行MQTT讀取变量（%s）失败，时间格式不正确(%s)！", pKey, szValueTime);
				return 0;
			}
			t2 = CTime(t1.GetYear(), t1.GetMonth(), t1.GetDay(), t1.GetHour(), t1.GetMinute(), t1.GetSecond());
			m_MapTopicDst.SetAt(pItem->szDstOPCPath, pKey);
			//
			SYSTEMTIME  lt, st;
			TIME_ZONE_INFORMATION zinfo;
			GetTimeZoneInformation(&zinfo);

			t2.GetAsSystemTime(lt);
			::TzSpecificLocalTimeToSystemTime(&zinfo, &lt, &st);
			::SystemTimeToFileTime(&st, &(pItem->dataTime));
			//
			pItem->dblValue = dblValue;
			pItem->quality = OPC_QUALITY_GOOD;
			pItem->bUpdateFlag = 0;
			if (stricmp(pMqttTunnelItem->szType, "ThingsDataLink") == 0)
			{
				if (strlen(pMqttTunnelItem->szMasteHost) > 0)
				{
					pItem->bUpdateFlag++;
				}
				if (strlen(pMqttTunnelItem->szSlaveHost) > 0)
				{
					pItem->bUpdateFlag++;
				}
			}
			else
			{
				pItem->bUpdateFlag++;
			}
			//
			CMqttTunnel *pMqttTunnel = (CMqttTunnel *)m_pMqttHandle;
			struct TunnelValueItem info1;
			memset(&info1, 0, sizeof(struct TunnelValueItem));
			info1.id = pItem->id;
			strcpy(info1.no, pItem->no);
			strcpy(info1.szOPCPath, pItem->szDstOPCPath);
			info1.dblValue = pItem->dblValue;
			info1.TimeStamp = pItem->dataTime;
			info1.ValueType = VT_R8;
			if (pMqttTunnel->WriteItem(&info1) == 0)
			{
				LogEvent("写入本地变量成功（%s=%f).....", pItem->szDstOPCPath, dblValue);
			}
			else
			{

				LogEvent("写入本地变量失敗（%s=%f).....", pItem->szDstOPCPath, dblValue);
			}
		}
		else
		{
			CString szKey, szPayload;
			if (stricmp(pMqttTunnelItem->szMqttFormat, "BIN") == 0)
			{
				szPayload = "[";
				char *pData = strDataGBK.GetBuffer();
				for (int i = 0; i < strlen(pData); i++)
				{
					CString szTemp;
					szTemp.Format("%d,", (byte)pData[i]);
					szPayload += szTemp;
				}
				//
				strDataGBK.ReleaseBuffer();
				if (szPayload.GetLength()>1)
					szPayload = szPayload.Mid(0, szPayload.GetLength() - 1);
				szPayload += "]";
			}
			else
			{
				szPayload = strDataGBK;
			}
			m_MapPayload.SetAt(pKey, szPayload);
			//
			LogEvent("【北向来报】topic=%s，payload=%s", pKey, szPayload);
			//
			CString szParseResult = "";
			CMqttTunnel *pMqttTunnel = (CMqttTunnel *)m_pMqttHandle;
			if (pMqttTunnel->ParseMqttPayload(pMqttTunnelItem->szReadParseCode, pKey, pMqttTunnelItem->szMqttFormat, szPayload, szPayload.GetLength(), szParseResult) == -1)
			{
				LogEvent("北向MQTT讀取变量（%s）失败，数据解析不正确或返回为空！", pKey);
				return 0;
			}
			//
			LogEvent("【北向解析】topic=%s，payload=%s", pKey, szParseResult);
			//
			CString resToken;
			int curPos = 0;
			resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
			while (resToken != _T(""))
			{
				resToken.TrimLeft();
				if (resToken == _T(""))
				{
					resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
					continue;
				}
				//
				CString szTopicTag, szValueTime, szValueData;
				szTopicTag = resToken.Mid(0, resToken.Find('|'));
				resToken = resToken.Mid(resToken.Find('|') + 1);
				szValueTime = resToken.Mid(0, resToken.Find('|'));
				szValueTime.Replace("T", " ");
				szValueData = resToken.Mid(resToken.Find('|') + 1);
				//
				COleDateTime tc;
				if (!tc.ParseDateTime(szValueTime))
				{
					LogEvent("北向MQTT讀取变量（%s）失败，时间格式不正确(%s)！", pKey, resToken);
					return 0;
				}
				CTime t2 = CTime(tc.GetYear(), tc.GetMonth(), tc.GetDay(), tc.GetHour(), tc.GetMinute(), tc.GetSecond());
				//
				struct MqttToOPCValueItem	*pItem = NULL;
				if (m_MqttMapTable1.Lookup(szTopicTag, pItem) == 0)
				{
					resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
					continue;
				}
				m_MapTopicDst.SetAt(pItem->szDstOPCPath, pKey);
				m_MapPayload.SetAt(szTopicTag, szPayload);
				//
				LogEvent("【北向变量】%s—>%s = %f %s", pItem->szDstOPCPath, szTopicTag, dblValue, pItem->szUnit);
				//
				SYSTEMTIME  lt, st;
				TIME_ZONE_INFORMATION zinfo;
				GetTimeZoneInformation(&zinfo);

				t2.GetAsSystemTime(lt);
				::TzSpecificLocalTimeToSystemTime(&zinfo, &lt, &st);
				::SystemTimeToFileTime(&st, &(pItem->dataTime));
				//
				pItem->dblValue = dblValue;
				pItem->quality = OPC_QUALITY_GOOD;
				pItem->bUpdateFlag = 0;
				if (stricmp(pMqttTunnelItem->szType, "ThingsDataLink") == 0)
				{
					if (strlen(pMqttTunnelItem->szMasteHost) > 0)
					{
						pItem->bUpdateFlag++;
					}
					if (strlen(pMqttTunnelItem->szSlaveHost) > 0)
					{
						pItem->bUpdateFlag++;
					}
				}
				else
				{
					pItem->bUpdateFlag++;
				}
				//
				CMqttTunnel *pMqttTunnel = (CMqttTunnel *)m_pMqttHandle;
				struct TunnelValueItem info1;
				memset(&info1, 0, sizeof(struct TunnelValueItem));
				info1.id = pItem->id;
				strcpy(info1.no, pItem->no);
				strcpy(info1.szOPCPath, pItem->szDstOPCPath);
				info1.dblValue = pItem->dblValue;
				info1.TimeStamp = pItem->dataTime;
				info1.ValueType = VT_R8;
				if (pMqttTunnel->WriteItem(&info1) == 0)
				{
					LogEvent("写入本地变量成功（%s=%f).....", pItem->szDstOPCPath, dblValue);
				}
				else
				{

					LogEvent("写入本地变量失敗（%s=%f).....", pItem->szDstOPCPath, dblValue);
				}
				//
				resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
			}
		}
	}
	return 0;
}

CMqttSmart::CMqttSmart(const char *id, const char *host, int port, const char *username, const char *password) : mosquittopp(id)
{
	int keepalive = 60;
	m_pMqttHandle = NULL; 
	m_bConnectFlag = 0;

	m_MapTopicValue.InitHashTable(10001);
	m_MapPayload.InitHashTable(10001);
	m_MapTopicSrc.InitHashTable(10001);
	m_MapTopicDst.InitHashTable(10001);

	m_MqttMapTable1.InitHashTable(10001);
	m_MqttMapTable2.InitHashTable(10001);

	if (strlen(username) > 0)
		username_pw_set(username, password);
	connect(host, port, keepalive);

};

CMqttSmart::~CMqttSmart()
{
}

void CMqttSmart::on_connect(int rc)
{
	if (rc == 0) {
		/* Only attempt to subscribe on a successful connect. */
		subscribe(NULL, pMqttTunnelItem->szMqttFilter);
	}
}

void CMqttSmart::on_message(const struct mosquitto_message *message)
{
	if (message->payloadlen > 0)
	{
		DWORD uTime;
		double dblValue = 0;
		CTime t2 = CTime::GetCurrentTime();
		struct MqttToOPCValueItem	*pItem = NULL;
		//
		CString strDataGBK = _T("");
		if (stricmp(pMqttTunnelItem->szCode, "UTF8") == 0)
		{
			strDataGBK = ConvertUtf8ToGBK((char *)(message->payload));
		}
		else if (stricmp(pMqttTunnelItem->szCode, "Unicode") == 0)
		{
			strDataGBK = ConvertUtf16ToGBK((char *)(message->payload));
		}
		else
		{
			strDataGBK = (char *)(message->payload);
		}
		//
		if (_stricmp(pMqttTunnelItem->szMqttFormat, "TVU") == 0)
		{
			if (m_MqttMapTable1.Lookup(message->topic, pItem) == 0)
			{
				return;
			}
			//
			if (pItem->bUpdateFlag > 0)
			{
				pItem->bUpdateFlag--;
				return;
			}
			//
			CString valFormat = "%19s|%lf|U";
			TCHAR szValueTime[32];
			int ret = sscanf_s(strDataGBK, valFormat, szValueTime, 32, &dblValue);
			if (ret == -1)
			{
				LogEvent("上行MQTT讀取变量（%s）失败，数据格式不正确(%s)！", message->topic, valFormat);
				return;
			}
			szValueTime[10] = ' ';
			//
			COleDateTime t1;
			if (!t1.ParseDateTime(szValueTime))
			{
				LogEvent("上行MQTT讀取变量（%s）失败，时间格式不正确(%s)！", message->topic, szValueTime);
				return;
			}
			t2 = CTime(t1.GetYear(), t1.GetMonth(), t1.GetDay(), t1.GetHour(), t1.GetMinute(), t1.GetSecond());
			m_MapTopicDst.SetAt(pItem->szDstOPCPath,message->topic);
			//
			SYSTEMTIME  lt, st;
			TIME_ZONE_INFORMATION zinfo;
			GetTimeZoneInformation(&zinfo);

			t2.GetAsSystemTime(lt);
			::TzSpecificLocalTimeToSystemTime(&zinfo, &lt, &st);
			::SystemTimeToFileTime(&st, &(pItem->dataTime));
			//
			pItem->dblValue = dblValue;
			pItem->quality = OPC_QUALITY_GOOD;
			pItem->bUpdateFlag = 0;
			if (stricmp(pMqttTunnelItem->szType, "ThingsDataLink") == 0)
			{
				if (strlen(pMqttTunnelItem->szMasteHost) > 0)
				{
					pItem->bUpdateFlag++;
				}
				if (strlen(pMqttTunnelItem->szSlaveHost) > 0)
				{
					pItem->bUpdateFlag++;
				}
			}
			else
			{
				pItem->bUpdateFlag++;
			}
			//
			CMqttTunnel *pMqttTunnel = (CMqttTunnel *)m_pMqttHandle;
			struct TunnelValueItem info1;
			memset(&info1, 0, sizeof(struct TunnelValueItem));
			info1.id = pItem->id;
			strcpy(info1.no, pItem->no);
			strcpy(info1.szOPCPath, pItem->szDstOPCPath);
			info1.dblValue = pItem->dblValue;
			info1.TimeStamp = pItem->dataTime;
			info1.ValueType = VT_R8;
			if (pMqttTunnel->WriteItem(&info1) == 0)
			{
				LogEvent("写入本地变量成功（%s=%f).....", pItem->szDstOPCPath, dblValue);
			}
			else
			{

				LogEvent("写入本地变量失敗（%s=%f).....", pItem->szDstOPCPath, dblValue);
			}
		}
		else
		{
			CString szKey, szPayload;
			if (stricmp(pMqttTunnelItem->szMqttFormat, "BIN") == 0)
			{
				szPayload = "[";
				char *pData = strDataGBK.GetBuffer();
				for (int i = 0; i < strlen(pData); i++)
				{
					CString szTemp;
					szTemp.Format("%d,", (byte)pData[i]);
					szPayload += szTemp;
				}
				//
				strDataGBK.ReleaseBuffer();
				if (szPayload.GetLength()>1)
					szPayload = szPayload.Mid(0, szPayload.GetLength() - 1);
				szPayload += "]";
			}
			else
			{
				szPayload = strDataGBK;
			}
			m_MapPayload.SetAt(message->topic, szPayload);
			//
			LogEvent("【北向来报】topic=%s，payload=%s", message->topic, szPayload);
			//
			CString szParseResult = "";
			CMqttTunnel *pMqttTunnel = (CMqttTunnel *)m_pMqttHandle;
			if (pMqttTunnel->ParseMqttPayload(pMqttTunnelItem->szReadParseCode, message->topic, pMqttTunnelItem->szMqttFormat, szPayload, szPayload.GetLength(), szParseResult) == -1)
			{
				LogEvent("北向MQTT讀取变量（%s）失败，数据解析不正确或返回为空！", message->topic);
				return;
			}
			//
			LogEvent("【北向解析】topic=%s，payload=%s", message->topic, szParseResult);
			//
			CString resToken;
			int curPos = 0;
			resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
			while (resToken != _T(""))
			{
				resToken.TrimLeft();
				if (resToken == _T(""))
				{
					resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
					continue;
				}
				//
				CString szTopicTag, szValueTime, szValueData;
				szTopicTag = resToken.Mid(0, resToken.Find('|'));
				resToken = resToken.Mid(resToken.Find('|') + 1);
				szValueTime = resToken.Mid(0, resToken.Find('|'));
				szValueTime.Replace("T", " ");
				szValueData = resToken.Mid(resToken.Find('|') + 1);
				//
				COleDateTime tc;
				if (!tc.ParseDateTime(szValueTime))
				{
					LogEvent("北向MQTT讀取变量（%s）失败，时间格式不正确(%s)！", message->topic, resToken);
					return;
				}
				CTime t2 = CTime(tc.GetYear(), tc.GetMonth(), tc.GetDay(), tc.GetHour(), tc.GetMinute(), tc.GetSecond());
				//
				struct MqttToOPCValueItem	*pItem = NULL;
				if (m_MqttMapTable1.Lookup(szTopicTag, pItem) == 0)
				{
					resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
					continue;
				}
				m_MapTopicDst.SetAt(pItem->szDstOPCPath, message->topic);
				m_MapPayload.SetAt(szTopicTag, szPayload);
				//
				LogEvent("【北向变量】%s—>%s = %f %s", pItem->szDstOPCPath, szTopicTag, dblValue, pItem->szUnit);
				//
				SYSTEMTIME  lt, st;
				TIME_ZONE_INFORMATION zinfo;
				GetTimeZoneInformation(&zinfo);

				t2.GetAsSystemTime(lt);
				::TzSpecificLocalTimeToSystemTime(&zinfo, &lt, &st);
				::SystemTimeToFileTime(&st, &(pItem->dataTime));
				//
				pItem->dblValue = dblValue;
				pItem->quality = OPC_QUALITY_GOOD;
				pItem->bUpdateFlag = 0;
				if (stricmp(pMqttTunnelItem->szType, "ThingsDataLink") == 0)
				{
					if (strlen(pMqttTunnelItem->szMasteHost) > 0)
					{
						pItem->bUpdateFlag++;
					}
					if (strlen(pMqttTunnelItem->szSlaveHost) > 0)
					{
						pItem->bUpdateFlag++;
					}
				}
				else
				{
					pItem->bUpdateFlag++;
				}
				//
				CMqttTunnel *pMqttTunnel = (CMqttTunnel *)m_pMqttHandle;
				struct TunnelValueItem info1;
				memset(&info1, 0, sizeof(struct TunnelValueItem));
				info1.id = pItem->id;
				strcpy(info1.no, pItem->no);
				strcpy(info1.szOPCPath, pItem->szDstOPCPath);
				info1.dblValue = pItem->dblValue;
				info1.TimeStamp = pItem->dataTime;
				info1.ValueType = VT_R8;
				if (pMqttTunnel->WriteItem(&info1) == 0)
				{
					LogEvent("写入本地变量成功（%s=%f).....", pItem->szDstOPCPath, dblValue);
				}
				else
				{

					LogEvent("写入本地变量失敗（%s=%f).....", pItem->szDstOPCPath, dblValue);
				}
				//
				resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
			}
		}
	}
}

void CMqttSmart::on_subscribe(int mid, int qos_count, const int *granted_qos)
{
	m_bConnectFlag = 1;
}

CMqttTunnel::CMqttTunnel(const char *id, const char *host, int port, const char *username, const char *password, const char *topic) : mosquittopp(id)
{
	int keepalive = 60;
	m_ParseTryCount = 0;
	m_pMqttHandle = NULL;
	sMeter = INVALID_SOCKET;
	hSession = NULL;
	hConnect = NULL;

	memset(m_szMqttID, 0, 64);
	sprintf_s(m_szMqttID, "%s", id);

	memset(m_szTopic, 0, 64);
	sprintf_s(m_szTopic, "%s", topic);

	hSockMutex = CreateMutex(NULL, FALSE, NULL);

	m_MapTopicValue.InitHashTable(10001);
	m_MapPayload.InitHashTable(10001);
	m_MapTopicSrc.InitHashTable(10001);
	m_MapTopicDst.InitHashTable(10001);

	m_MqttMapTable1.InitHashTable(10001);
	m_MqttMapTable2.InitHashTable(10001);
	m_MqttMapTable3.InitHashTable(10001);
	m_MqttMapTable4.InitHashTable(10001);
	bHisFlag = 0;
	nHisCount = 0;

	if (strlen(username) > 0)
		username_pw_set(username, password);
	connect(host, port, keepalive);
};

CMqttTunnel::~CMqttTunnel()
{
}

void CMqttTunnel::on_connect(int rc)
{
	if (rc == 0) {
		/* Only attempt to subscribe on a successful connect. */
		subscribe(NULL, m_szTopic);
		LogEvent("MQTT subscribe with %s.", m_szTopic);
	}
}

void CMqttTunnel::on_message(const struct mosquitto_message *message)
{
	if (stricmp(pMqttTunnelItem->szDirect, "In") == 0)
	{
		return;
	}
	//
	if (message->payloadlen > 0)
	{
		CString strDataGBK = _T("");
		if (stricmp(pMqttTunnelItem->szMqttSlaveCode, "UTF8") == 0)
		{
			strDataGBK = ConvertUtf8ToGBK((char *)(message->payload));
		}
		else if (stricmp(pMqttTunnelItem->szMqttSlaveCode, "Unicode") == 0)
		{
			strDataGBK = ConvertUtf16ToGBK((char *)(message->payload));
		}
		else
		{
			strDataGBK = (char *)(message->payload);
		}
		//
		if (stricmp(pMqttTunnelItem->szDirect, "Forward") == 0)
		{
			if (stricmp(pMqttTunnelItem->szType, "MQTT") == 0)
			{
				CString path = pMqttTunnelItem->szMqttFilter;
				path.Replace("#", message->topic);
				//
				CString val = "";
				if (stricmp(pMqttTunnelItem->szCode, "UTF8") == 0)
					val = ConvertGBKToUtf8(strDataGBK);
				else if (stricmp(pMqttTunnelItem->szCode, "Unicode") == 0)
					val = ConvertGBKToUtf16(strDataGBK);
				//
				CMqttSmart *pMqttSmart = (CMqttSmart *)m_pMqttHandle;
				int rc=pMqttSmart->publish(NULL, path, val.GetLength(), val, message->qos, true);
				//
				struct MapValueItem myValue;
				memset(&myValue, 0, sizeof(struct MapValueItem));
				myValue.TunnelID = pMqttTunnelItem->TunnelID;
				myValue.ValueID = 3;
				myValue.nUpFlag = 1;
				myValue.nDataType = 0;
				myValue.fDataValue = (rc != MOSQ_ERR_SUCCESS?0:1);
				sprintf(myValue.szNodePath, "*");
				SendDataToMeter(&myValue);
				//
				_Module.m_UpdateTime = CTime::GetCurrentTime();
				//
				path.ReleaseBuffer();
				val.ReleaseBuffer();
				return;
			}
			else if (stricmp(pMqttTunnelItem->szType, "AMQP") == 0)
			{
				CString path = message->topic;
				CString val = "";
				if (stricmp(pMqttTunnelItem->szCode, "UTF8") == 0)
					val = ConvertGBKToUtf8(strDataGBK);
				else if (stricmp(pMqttTunnelItem->szCode, "Unicode") == 0)
					val = ConvertGBKToUtf16(strDataGBK);
				//
				CAmqpSmart *pAmqpSmart = (CAmqpSmart *)m_pAmqpHandle;
				DWORD rc = pAmqpSmart->publish(path.GetBuffer(), val.GetBuffer());
				//
				struct MapValueItem myValue;
				memset(&myValue, 0, sizeof(struct MapValueItem));
				myValue.TunnelID = pMqttTunnelItem->TunnelID;
				myValue.ValueID = 3;
				myValue.nUpFlag = 1;
				myValue.nDataType = 0;
				myValue.fDataValue = (rc==0?1:0);
				sprintf(myValue.szNodePath, "*");
				SendDataToMeter(&myValue);
				//
				_Module.m_UpdateTime = CTime::GetCurrentTime();
				//
				path.ReleaseBuffer();
				val.ReleaseBuffer();
				return;
			}
			else if (stricmp(pMqttTunnelItem->szType, "Kafka") == 0)
			{
				CString path = message->topic;
				CString val = strDataGBK;
				//
				ProducerKafka *pProducerKafka = (ProducerKafka *)m_pKafkaHandle;
				if (!pProducerKafka->m_bConnectFlag)
				{
					if (strlen(pMqttTunnelItem->szProducerTopic)>0
						&& PRODUCER_INIT_SUCCESS == pProducerKafka->init_kafka(RD_KAFKA_PARTITION_UA, pMqttTunnelItem->szKafkaBroker, pMqttTunnelItem->szKafkaUserName, pMqttTunnelItem->szKafkaPassWord, pMqttTunnelItem->szProducerTopic))
					{
						pProducerKafka->m_bConnectFlag = 1;
						if (pProducerKafka->push_data_to_kafka(val.GetBuffer(), val.GetLength(), path.GetBuffer(), path.GetLength()) == -1)
						{
							pProducerKafka->destroy();
							pProducerKafka->m_bConnectFlag = 0;
						}
					}
					else
					{
						pProducerKafka->m_bConnectFlag = 0;
					}
				}
				else
				{
					if (pProducerKafka->push_data_to_kafka(val.GetBuffer(), val.GetLength(), path.GetBuffer(), path.GetLength()) == -1)
					{
						pProducerKafka->destroy();
						pProducerKafka->m_bConnectFlag = 0;
					}
				}
				//
				struct MapValueItem myValue;
				memset(&myValue, 0, sizeof(struct MapValueItem));
				myValue.TunnelID = pMqttTunnelItem->TunnelID;
				myValue.ValueID = 3;
				myValue.nUpFlag = pProducerKafka->m_bConnectFlag;
				myValue.nDataType = 0;
				myValue.fDataValue = 1;
				sprintf(myValue.szNodePath, "*");
				SendDataToMeter(&myValue);
				//
				_Module.m_UpdateTime = CTime::GetCurrentTime();
				//
				path.ReleaseBuffer();
				val.ReleaseBuffer();
				return;
			}
		}
		//
		if (stricmp(pMqttTunnelItem->szMqttSlaveFormat,"TVU")==0)
		{
			CTime t2 = CTime::GetCurrentTime();
			double dblValue = 0;
			CString valFormat = "%19s|%lf|U";
			TCHAR szValueTime[32];
			int ret = sscanf_s(strDataGBK, valFormat, szValueTime, 32, &dblValue);
			if (ret == -1)
			{
				LogEvent("上行MQTT讀取变量（%s）失败，数据格式不正确(%s)！", message->topic, message->payload);
				return;
			}
			szValueTime[10] = ' ';
			//
			COleDateTime tc;
			if (!tc.ParseDateTime(szValueTime))
			{
				LogEvent("上行MQTT讀取变量（%s）失败，时间格式不正确(%s)！", message->topic, szValueTime);
				return;
			}
			t2 = CTime(tc.GetYear(), tc.GetMonth(), tc.GetDay(), tc.GetHour(), tc.GetMinute(), tc.GetSecond());
			//
			WaitForSingleObject(pMqttTunnelItem->hReadOPCMutex, INFINITE);
			CString szKey = "";
			DOUBLE keyValue;
			szKey.Format("%lu/%s", pMqttTunnelItem->TunnelID, message->topic);
			if (m_MapTopicValue.Lookup(szKey, keyValue) == 0)
			{
				WriteCSVFile(pMqttTunnelItem->TunnelID, FALSE, "%s,%s,%f", message->topic, t2.Format("%Y-%m-%d %H:%M:%S"), dblValue);
			}
			m_MapTopicValue[szKey] = dblValue;
			//
			struct MqttToOPCValueItem	*pItem = NULL;
			if (m_MqttMapTable1.Lookup(message->topic, pItem) == 0)
			{
				ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
				return;
			}
			//
			if (pItem->bUpdateFlag > 0)
			{
				pItem->quality = OPC_QUALITY_BAD;
			}
			//
			dblValue = dblValue*pItem->fDiv;
			dblValue += pItem->fDlt;
			//
			CTime t1 = CTime(pItem->dataTime);
			CTimeSpan ts = t2 - t1;
			if (pItem->quality == OPC_QUALITY_BAD
				|| (pItem->bMonitorValue
					&& ((pItem->dblValue != dblValue && fabs(pItem->dblValue - dblValue) > pItem->fDead)
						|| ts.GetTotalSeconds() >0
						|| (t1.GetMinute() % 15 == 0 && ts.GetTotalMinutes() > 1)))
				|| (!pItem->bMonitorValue
					&& (pItem->dblValue != dblValue && fabs(pItem->dblValue - dblValue) > pItem->fDead)))
			{
				SYSTEMTIME  lt, st;
				TIME_ZONE_INFORMATION zinfo;
				GetTimeZoneInformation(&zinfo);

				t2.GetAsSystemTime(lt);
				::TzSpecificLocalTimeToSystemTime(&zinfo, &lt, &st);
				::SystemTimeToFileTime(&st, &(pItem->dataTime));
				//
				pItem->dblValue = dblValue;
				pItem->quality = OPC_QUALITY_GOOD;
				if (pItem->bUpdateFlag <= 0)
				{
					pItem->bUpdateFlag = 0;
					if (stricmp(pMqttTunnelItem->szType, "ThingsDataLink") == 0)
					{
						if (strlen(pMqttTunnelItem->szMasteHost) > 0)
						{
							pItem->bUpdateFlag++;
						}
						if (strlen(pMqttTunnelItem->szSlaveHost) > 0)
						{
							pItem->bUpdateFlag++;
						}
					}
					else
					{
						pItem->bUpdateFlag++;
					}
				}
				//
				if (stricmp(pMqttTunnelItem->szType, "ThingsDataLink") == 0)
				{
					if (ts.GetTotalSeconds() > 0)
					{
						struct MapValueItem myValue;
						memset(&myValue, 0, sizeof(struct MapValueItem));
						myValue.TunnelID = pMqttTunnelItem->TunnelID;
						myValue.ValueID = pItem->id;
						myValue.nUpFlag = (m_ServerSock == INVALID_SOCKET ? 0 : 1);
						myValue.nDataType = VT_R8;
						myValue.fDataValue = pItem->dblValue;
						myValue.nDataTime = pItem->dataTime;
						//
						CString szDstOPCPath = pItem->szDstOPCPath;
						int offset = szDstOPCPath.ReverseFind('.');
						if (offset > 0)
						{
							sprintf_s(myValue.szNodePath, "%s", szDstOPCPath.Mid(0, offset));
						}
						//
						SendDataToMeter(&myValue);
					}
					//
					struct TunnelValueItem valItem;
					memset(&valItem, 0, sizeof(struct TunnelValueItem));
					valItem.id = pItem->id;
					sprintf_s(valItem.no, "%s", pItem->no);
					sprintf_s(valItem.szOPCPath, "%s", pItem->szDstOPCPath);
					valItem.TimeStamp = pItem->dataTime;
					valItem.dblValue = pItem->dblValue;
					valItem.ValueType = VT_R8;
					valItem.TunnelID = pMqttTunnelItem->TunnelID;
					//
					UpdateItem(&valItem);
					//
					pItem->bUpdateFlag--;
				}
				else if(stricmp(pMqttTunnelItem->szType, "MQTT") == 0)
				{
					CMqttSmart *pMqttSmart = (CMqttSmart *)m_pMqttHandle;
					//
					if (ts.GetTotalSeconds() > 0)
					{
						struct MapValueItem myValue;
						memset(&myValue, 0, sizeof(struct MapValueItem));
						myValue.TunnelID = pMqttTunnelItem->TunnelID;
						myValue.ValueID = pItem->id;
						myValue.nUpFlag = pMqttSmart->loop()>0 ? 0 : 1;
						myValue.nDataType = VT_R8;
						myValue.fDataValue = pItem->dblValue;
						myValue.nDataTime = pItem->dataTime;
						//
						CString szDstOPCPath = pItem->szDstOPCPath;
						int offset = szDstOPCPath.ReverseFind('.');
						if (offset > 0)
						{
							sprintf_s(myValue.szNodePath, "%s", szDstOPCPath.Mid(0, offset));
						}
						//
						SendDataToMeter(&myValue);
					}
					//
					CString path = "";
					if (m_MapTopicDst.Lookup(pItem->szDstOPCPath, path) == 0)
					{
						path = pItem->szDstOPCPath;
						path.Replace('.', '/');
					}
					//
					CString val = "";
					CTime tc(pItem->dataTime);
					if (stricmp(pMqttTunnelItem->szMqttFormat, "TVU") == 0)
					{
						val.Format("%s|%s|%s", tc.Format("%Y-%m-%dT%H:%M:%S"), FormatValueType(pItem->dblValue, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
					}
					else
					{
						CString szPayload = "";
						CTime tc(pItem->dataTime);
						szPayload.Format("{\"DataTime\":\"%s\",\"DataValue\":%s,\"DataUnit\":\"%s\"}", tc.Format("%Y-%m-%d %H:%M:%S"), FormatValueType(pItem->dblValue, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
						//
						if (ParseMqttPayload(pMqttTunnelItem->szWriteParseCode, pItem->szSrcTopic, "Publish", szPayload, szPayload.GetLength(), val) == -1)
						{
							LogEvent("写入上行MQTT变量（%s）失败，数据解析不正确(%s)！", message->topic, message->payload);
							return;
						}
						int offset = val.Find("|");
						if (offset>0)
						{
							path = val.Mid(0, offset);
							val = val.Mid(offset+1);
						}
					}
					//
					//
					if (stricmp(pMqttTunnelItem->szCode, "UTF8") == 0)
						val = ConvertGBKToUtf8(val);
					else if (stricmp(pMqttTunnelItem->szCode, "Unicode") == 0)
						val = ConvertGBKToUtf16(val);
					//
					pMqttSmart->publish(NULL, path, val.GetLength(), val, message->qos, true);
				}
				else if (stricmp(pMqttTunnelItem->szType, "AMQP") == 0)
				{
					CAmqpSmart *pAmqpSmart = (CAmqpSmart *)m_pAmqpHandle;
					//
					if (ts.GetTotalSeconds() > 0)
					{
						struct MapValueItem myValue;
						memset(&myValue, 0, sizeof(struct MapValueItem));
						myValue.TunnelID = pMqttTunnelItem->TunnelID;
						myValue.ValueID = pItem->id;
						myValue.nUpFlag = pAmqpSmart->m_bConnectFlag;
						myValue.nDataType = VT_R8;
						myValue.fDataValue = pItem->dblValue;
						myValue.nDataTime = pItem->dataTime;
						//
						CString szDstOPCPath = pItem->szDstOPCPath;
						int offset = szDstOPCPath.ReverseFind('.');
						if (offset > 0)
						{
							sprintf_s(myValue.szNodePath, "%s", szDstOPCPath.Mid(0, offset));
						}
						//
						SendDataToMeter(&myValue);
					}
					//
					CString path = "";
					if (m_MapTopicDst.Lookup(pItem->szDstOPCPath, path) == 0)
					{
						path = pItem->szDstOPCPath;
						path.Replace('.', '/');
					}
					//
					CString val = "";
					CTime tc(pItem->dataTime);
					if (stricmp(pMqttTunnelItem->szAmqpFormat, "TVU") == 0)
					{
						val.Format("%s|%s|%s", tc.Format("%Y-%m-%dT%H:%M:%S"), FormatValueType(pItem->dblValue, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
					}
					else
					{
						CString szPayload = "";
						CTime tc(pItem->dataTime);
						szPayload.Format("{\"DataTime\":\"%s\",\"DataValue\":%s,\"DataUnit\":\"%s\"}", tc.Format("%Y-%m-%d %H:%M:%S"), FormatValueType(pItem->dblValue, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
						//
						if (ParseMqttPayload(pMqttTunnelItem->szWriteParseCode, pItem->szSrcTopic, "Publish", szPayload, szPayload.GetLength(), val) == -1)
						{
							LogEvent("写入上行MQTT变量（%s）失败，数据解析不正确(%s)！", message->topic, message->payload);
							return;
						}
						int offset = val.Find("|");
						if (offset>0)
						{
							path = val.Mid(0, offset);
							val = val.Mid(offset + 1);
						}
					}
					//
					if (stricmp(pMqttTunnelItem->szCode, "UTF8") == 0)
						val = ConvertGBKToUtf8(val);
					else if (stricmp(pMqttTunnelItem->szCode, "Unicode") == 0)
						val = ConvertGBKToUtf16(val);
					//
					pAmqpSmart->publish(path.GetBuffer(),val.GetBuffer());
					path.ReleaseBuffer();
					val.ReleaseBuffer();
				}
				else if (stricmp(pMqttTunnelItem->szType, "Kafka") == 0)
				{
					ConsummerKafka*	pConsummerKafka = (ConsummerKafka *)m_pConsummerHandle;
					ProducerKafka *pProducerKafka = (ProducerKafka *)m_pKafkaHandle;
					//
					if (ts.GetTotalSeconds() > 0)
					{
						struct MapValueItem myValue;
						memset(&myValue, 0, sizeof(struct MapValueItem));
						myValue.TunnelID = pMqttTunnelItem->TunnelID;
						myValue.ValueID = pItem->id;
						myValue.nUpFlag = pConsummerKafka->m_bConnectFlag;
						myValue.nDataType = VT_R8;
						myValue.fDataValue = pItem->dblValue;
						myValue.nDataTime = pItem->dataTime;
						//
						CString szDstOPCPath = pItem->szDstOPCPath;
						int offset = szDstOPCPath.ReverseFind('.');
						if (offset > 0)
						{
							sprintf_s(myValue.szNodePath, "%s", szDstOPCPath.Mid(0, offset));
						}
						//
						SendDataToMeter(&myValue);
					}
					//
					CString path = "";
					if (m_MapTopicDst.Lookup(pItem->szDstOPCPath, path) == 0)
					{
						path = pItem->szDstOPCPath;
						path.Replace('.', '/');
					}
					//
					CString val = "";
					CTime tc(pItem->dataTime);
					if (stricmp(pMqttTunnelItem->szAmqpFormat, "TVU") == 0)
					{
						val.Format("%s|%s|%s", tc.Format("%Y-%m-%dT%H:%M:%S"), FormatValueType(pItem->dblValue, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
					}
					else
					{
						CString szPayload = "";
						CTime tc(pItem->dataTime);
						szPayload.Format("{\"DataTime\":\"%s\",\"DataValue\":%s,\"DataUnit\":\"%s\"}", tc.Format("%Y-%m-%d %H:%M:%S"), FormatValueType(pItem->dblValue, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
						//
						if (ParseMqttPayload(pMqttTunnelItem->szWriteParseCode, pItem->szSrcTopic, "Publish", szPayload, szPayload.GetLength(), val) == -1)
						{
							LogEvent("写入上行MQTT变量（%s）失败，数据解析不正确(%s)！", message->topic, message->payload);
							return;
						}
						int offset = val.Find("|");
						if (offset>0)
						{
							path = val.Mid(0, offset);
							val = val.Mid(offset + 1);
						}
					}
					//
					if (!pProducerKafka->m_bConnectFlag)
					{
						if (strlen(pMqttTunnelItem->szProducerTopic)>0
							&& PRODUCER_INIT_SUCCESS == pProducerKafka->init_kafka(RD_KAFKA_PARTITION_UA, pMqttTunnelItem->szKafkaBroker, pMqttTunnelItem->szKafkaUserName, pMqttTunnelItem->szKafkaPassWord, pMqttTunnelItem->szProducerTopic))
						{
							pProducerKafka->m_bConnectFlag = 1;
							if (pProducerKafka->push_data_to_kafka(val.GetBuffer(), val.GetLength(), path.GetBuffer(), path.GetLength()) == -1)
							{
								pProducerKafka->destroy();
								pProducerKafka->m_bConnectFlag = 0;
							}
						}
						else
						{
							pProducerKafka->m_bConnectFlag = 0;
						}
					}
					else
					{
						if (pProducerKafka->push_data_to_kafka(val.GetBuffer(), val.GetLength(), path.GetBuffer(), path.GetLength()) == -1)
						{
							pProducerKafka->destroy();
							pProducerKafka->m_bConnectFlag = 0;
						}
					}
					path.ReleaseBuffer();
					val.ReleaseBuffer();
				}
			}
			//
			ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
		}
		else
		{
			CString szKey,szPayload;
			if (stricmp(pMqttTunnelItem->szMqttSlaveFormat, "BIN") == 0)
			{
				szPayload = "[";
				char *pData = strDataGBK.GetBuffer();
				for (int i = 0; i < strlen(pData); i++)
				{
					CString szTemp;
					szTemp.Format("%d,", (byte)pData[i]);
					szPayload += szTemp;
				}
				//
				strDataGBK.ReleaseBuffer();
				if (szPayload.GetLength()>1)
					szPayload = szPayload.Mid(0, szPayload.GetLength() - 1);
				szPayload += "]";
			}
			else
			{
				szPayload = strDataGBK;
			}
			m_MapPayload.SetAt(message->topic, szPayload);
			//
			char *pReadBuffer = NULL;
			CString szParseResult = "";
			if (szPayload.GetLength()==0
				|| ParseMqttPayload(pMqttTunnelItem->szMqttReadParseCode, message->topic, pMqttTunnelItem->szMqttSlaveFormat, szPayload, szPayload.GetLength(),szParseResult) == -1)
			{
				LogEvent("下行MQTT读取变量（%s）失败，数据解析不正确或返回为空！", message->topic);
				return;
			}
			//
			int nReadCount = 0;
			int nReadLen = sizeof(struct TunnelValueItem) * 16;
			pReadBuffer = (char *)malloc(nReadLen);
			struct TunnelValueItem tValueData;
			//
			WaitForSingleObject(pMqttTunnelItem->hReadOPCMutex, INFINITE);
			CString resToken;
			int curPos = 0;
			resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
			while (resToken != _T(""))
			{
				resToken.TrimLeft();
				if (resToken == _T(""))
				{
					resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
					continue;
				}
				//
				CString szTopicTag, szValueTime, szValueData;
				szTopicTag = resToken.Mid(0, resToken.Find('|'));
				resToken = resToken.Mid(resToken.Find('|')+1);
				szValueTime= resToken.Mid(0, resToken.Find('|'));
				szValueTime.Replace("T"," ");
				szValueData = resToken.Mid(resToken.Find('|')+1);
				//
				COleDateTime tc;
				if (!tc.ParseDateTime(szValueTime))
				{
					LogEvent("下行MQTT读取变量（%s=%s）失败，时间格式不正确！", message->topic, szValueTime);
					break;
				}
				CTime t2 = CTime(tc.GetYear(), tc.GetMonth(), tc.GetDay(), tc.GetHour(), tc.GetMinute(), tc.GetSecond());
				//
				CString szKey = "";
				DOUBLE keyValue;
				szKey.Format("%lu/%s", pMqttTunnelItem->TunnelID, szTopicTag);
				double dblValue = atof(szValueData);
				if (m_MapTopicValue.Lookup(szKey, keyValue) == 0)
				{
					WriteCSVFile(pMqttTunnelItem->TunnelID, FALSE, "%s,%s,%f", szTopicTag, tc.Format("%Y-%m-%d %H:%M:%S"), dblValue);
				}
				//
				m_MapTopicValue[szKey] = dblValue;
				//
				struct MqttToOPCValueItem	*pItem = NULL;
				if (m_MqttMapTable1.Lookup(szTopicTag, pItem) == 0)
				{
					resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
					continue;
				}
				m_MapTopicSrc.SetAt(szTopicTag,message->topic);
				m_MapPayload.SetAt(szTopicTag, szPayload);
				//
				if (pItem->bUpdateFlag > 0)
				{
					pItem->quality = OPC_QUALITY_BAD;
				}
				dblValue = dblValue*pItem->fDiv;
				dblValue += pItem->fDlt;
				//
				CTime t1 = CTime(pItem->dataTime);
				CTimeSpan ts = t2 - t1;
				//
				if (pItem->quality == OPC_QUALITY_BAD
					|| (pItem->bMonitorValue
						&& ((pItem->dblValue != dblValue && fabs(pItem->dblValue - dblValue) > pItem->fDead)
							|| ts.GetTotalSeconds() >0
							|| (t1.GetMinute() % 15 == 0 && ts.GetTotalMinutes() > 1)))
					|| (!pItem->bMonitorValue
						&& (pItem->dblValue != dblValue && fabs(pItem->dblValue - dblValue) > pItem->fDead)))
				{
					SYSTEMTIME  lt, st;
					TIME_ZONE_INFORMATION zinfo;
					GetTimeZoneInformation(&zinfo);

					t2.GetAsSystemTime(lt);
					::TzSpecificLocalTimeToSystemTime(&zinfo, &lt, &st);
					::SystemTimeToFileTime(&st, &(pItem->dataTime));
					//
					pItem->dblValue = dblValue;
					pItem->quality = OPC_QUALITY_GOOD;
					if (pItem->bUpdateFlag <= 0)
					{
						pItem->bUpdateFlag = 0;
						if (stricmp(pMqttTunnelItem->szType, "ThingsDataLink") == 0)
						{
							if (strlen(pMqttTunnelItem->szMasteHost) > 0)
							{
								pItem->bUpdateFlag++;
							}
							if (strlen(pMqttTunnelItem->szSlaveHost) > 0)
							{
								pItem->bUpdateFlag++;
							}
						}
						else
						{
							pItem->bUpdateFlag++;
						}
					}
					//
					if (stricmp(pMqttTunnelItem->szType, "ThingsDataLink") == 0)
					{
						if (ts.GetTotalSeconds() > 0)
						{
							struct MapValueItem myValue;
							memset(&myValue, 0, sizeof(struct MapValueItem));
							myValue.TunnelID = pMqttTunnelItem->TunnelID;
							myValue.ValueID = pItem->id;
							myValue.nUpFlag = (m_ServerSock == INVALID_SOCKET ? 0 : 1);
							myValue.nDataType = VT_R8;
							myValue.fDataValue = pItem->dblValue;
							myValue.nDataTime = pItem->dataTime;
							//
							CString szDstOPCPath = pItem->szDstOPCPath;
							int offset = szDstOPCPath.ReverseFind('.');
							if (offset > 0)
							{
								sprintf_s(myValue.szNodePath, "%s", szDstOPCPath.Mid(0, offset));
							}
							//
							SendDataToMeter(&myValue);
						}
						//
						memset(&tValueData, 0, sizeof(struct TunnelValueItem));
						sprintf_s(tValueData.no, "%s", pItem->no);
						sprintf_s(tValueData.szOPCPath, "%s", pItem->szDstOPCPath);
						tValueData.id = pItem->id;
						tValueData.TunnelID = pMqttTunnelItem->TunnelID;
						tValueData.TimeStamp = pItem->dataTime;
						tValueData.ValueType = VT_R8;
						tValueData.dblValue = pItem->dblValue;
						//
						if (sizeof(struct TunnelValueItem)*(nReadCount + 1) > nReadLen)
						{
							nReadLen += sizeof(struct TunnelValueItem) * 16;
							pReadBuffer = (char *)realloc(pReadBuffer, nReadLen);
						}
						memcpy(pReadBuffer + sizeof(struct TunnelValueItem)*nReadCount, (char *)&tValueData, sizeof(struct TunnelValueItem));
						nReadCount++;
						//
						pItem->bUpdateFlag--;
					}
					else if (stricmp(pMqttTunnelItem->szType, "MQTT") == 0)
					{
						CMqttSmart *pMqttSmart = (CMqttSmart *)m_pMqttHandle;
						//
						if (ts.GetTotalSeconds() > 0)
						{
							struct MapValueItem myValue;
							memset(&myValue, 0, sizeof(struct MapValueItem));
							myValue.TunnelID = pMqttTunnelItem->TunnelID;
							myValue.ValueID = pItem->id;
							myValue.nUpFlag = pMqttSmart->loop()>0 ? 0 : 1;
							myValue.nDataType = VT_R8;
							myValue.fDataValue = pItem->dblValue;
							myValue.nDataTime = pItem->dataTime;
							//
							CString szDstOPCPath = pItem->szDstOPCPath;
							int offset = szDstOPCPath.ReverseFind('.');
							if (offset > 0)
							{
								sprintf_s(myValue.szNodePath, "%s", szDstOPCPath.Mid(0, offset));
							}
							//
							SendDataToMeter(&myValue);
						}
						//
						CString path = "";
						if (m_MapTopicDst.Lookup(pItem->szDstOPCPath, path) == 0)
						{
							path = pItem->szDstOPCPath;
							path.Replace('.', '/');
						}
						//
						CString val = "";
						CTime tc(pItem->dataTime);
						if (stricmp(pMqttTunnelItem->szMqttFormat, "TVU") == 0)
						{
							val.Format("%s|%s|%s", tc.Format("%Y-%m-%dT%H:%M:%S"), FormatValueType(pItem->dblValue, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
						}
						else
						{
							CString szPayload = "";
							CTime tc(pItem->dataTime);
							szPayload.Format("{\"DataTime\":\"%s\",\"DataValue\":%s,\"DataUnit\":\"%s\"}", tc.Format("%Y-%m-%d %H:%M:%S"), FormatValueType(pItem->dblValue, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
							//
							if (ParseMqttPayload(pMqttTunnelItem->szWriteParseCode, pItem->szSrcTopic, "Publish", szPayload, szPayload.GetLength(), val) == -1)
							{
								LogEvent("写入上行MQTT变量（%s）失败，数据解析不正确(%s)！", message->topic, message->payload);
								return;
							}
							int offset = val.Find("|");
							if (offset>0)
							{
								path = val.Mid(0, offset);
								val = val.Mid(offset + 1);
							}
						}
						//
						if (stricmp(pMqttTunnelItem->szCode, "UTF8") == 0)
							val = ConvertGBKToUtf8(val);
						else if (stricmp(pMqttTunnelItem->szCode, "Unicode") == 0)
							val = ConvertGBKToUtf16(val);
						//
						pMqttSmart->publish(NULL, path, val.GetLength(), val, message->qos, true);
					}
					else if (stricmp(pMqttTunnelItem->szType, "AMQP") == 0)
					{
						CAmqpSmart *pAmqpSmart = (CAmqpSmart *)m_pAmqpHandle;
						//
						if (ts.GetTotalSeconds() > 0)
						{
							struct MapValueItem myValue;
							memset(&myValue, 0, sizeof(struct MapValueItem));
							myValue.TunnelID = pMqttTunnelItem->TunnelID;
							myValue.ValueID = pItem->id;
							myValue.nUpFlag = pAmqpSmart->m_bConnectFlag;
							myValue.nDataType = VT_R8;
							myValue.fDataValue = pItem->dblValue;
							myValue.nDataTime = pItem->dataTime;
							//
							CString szDstOPCPath = pItem->szDstOPCPath;
							int offset = szDstOPCPath.ReverseFind('.');
							if (offset > 0)
							{
								sprintf_s(myValue.szNodePath, "%s", szDstOPCPath.Mid(0, offset));
							}
							//
							SendDataToMeter(&myValue);
						}
						//
						CString path = "";
						if (m_MapTopicDst.Lookup(pItem->szDstOPCPath, path) == 0)
						{
							path = pItem->szDstOPCPath;
							path.Replace('.', '/');
						}
						//
						CString val = "";
						CTime tc(pItem->dataTime);
						if (stricmp(pMqttTunnelItem->szAmqpFormat, "TVU") == 0)
						{
							val.Format("%s|%s|%s", tc.Format("%Y-%m-%dT%H:%M:%S"), FormatValueType(pItem->dblValue, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
						}
						else
						{
							CString szPayload = "";
							CTime tc(pItem->dataTime);
							szPayload.Format("{\"DataTime\":\"%s\",\"DataValue\":%s,\"DataUnit\":\"%s\"}", tc.Format("%Y-%m-%d %H:%M:%S"), FormatValueType(pItem->dblValue, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
							//
							if (ParseMqttPayload(pMqttTunnelItem->szWriteParseCode, pItem->szSrcTopic, "Publish", szPayload, szPayload.GetLength(), val) == -1)
							{
								LogEvent("写入上行MQTT变量（%s）失败，数据解析不正确(%s)！", message->topic, message->payload);
								return;
							}
							int offset = val.Find("|");
							if (offset>0)
							{
								path = val.Mid(0, offset);
								val = val.Mid(offset + 1);
							}
						}
						//
						if (stricmp(pMqttTunnelItem->szCode, "UTF8") == 0)
							val = ConvertGBKToUtf8(val);
						else if (stricmp(pMqttTunnelItem->szCode, "Unicode") == 0)
							val = ConvertGBKToUtf16(val);
						//
						pAmqpSmart->publish(path.GetBuffer(), val.GetBuffer());
						path.ReleaseBuffer();
						val.ReleaseBuffer();
					}
					else if (stricmp(pMqttTunnelItem->szType, "Kafka") == 0)
					{
						ProducerKafka *pProducerKafka = (ProducerKafka *)m_pKafkaHandle;
						//
						if (ts.GetTotalSeconds() > 0)
						{
							struct MapValueItem myValue;
							memset(&myValue, 0, sizeof(struct MapValueItem));
							myValue.TunnelID = pMqttTunnelItem->TunnelID;
							myValue.ValueID = pItem->id;
							myValue.nUpFlag = pProducerKafka->m_bConnectFlag;
							myValue.nDataType = VT_R8;
							myValue.fDataValue = pItem->dblValue;
							myValue.nDataTime = pItem->dataTime;
							//
							CString szDstOPCPath = pItem->szDstOPCPath;
							int offset = szDstOPCPath.ReverseFind('.');
							if (offset > 0)
							{
								sprintf_s(myValue.szNodePath, "%s", szDstOPCPath.Mid(0, offset));
							}
							//
							SendDataToMeter(&myValue);
						}
						//
						CString path = "";
						if (m_MapTopicDst.Lookup(pItem->szDstOPCPath, path) == 0)
						{
							path = pItem->szDstOPCPath;
							path.Replace('.', '/');
						}
						//
						CString val = "";
						CTime tc(pItem->dataTime);
						if (stricmp(pMqttTunnelItem->szAmqpFormat, "TVU") == 0)
						{
							val.Format("%s|%s|%s", tc.Format("%Y-%m-%dT%H:%M:%S"), FormatValueType(pItem->dblValue, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
						}
						else
						{
							CString szPayload = "";
							CTime tc(pItem->dataTime);
							szPayload.Format("{\"DataTime\":\"%s\",\"DataValue\":%s,\"DataUnit\":\"%s\"}", tc.Format("%Y-%m-%d %H:%M:%S"), FormatValueType(pItem->dblValue, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
							//
							if (ParseMqttPayload(pMqttTunnelItem->szWriteParseCode, pItem->szSrcTopic, "Publish", szPayload, szPayload.GetLength(), val) == -1)
							{
								LogEvent("写入上行MQTT变量（%s）失败，数据解析不正确(%s)！", message->topic, message->payload);
								return;
							}
							int offset = val.Find("|");
							if (offset>0)
							{
								path = val.Mid(0, offset);
								val = val.Mid(offset + 1);
							}
						}
						//
						if (!pProducerKafka->m_bConnectFlag)
						{
							if (strlen(pMqttTunnelItem->szProducerTopic)>0
								&& PRODUCER_INIT_SUCCESS == pProducerKafka->init_kafka(RD_KAFKA_PARTITION_UA, pMqttTunnelItem->szKafkaBroker, pMqttTunnelItem->szKafkaUserName, pMqttTunnelItem->szKafkaPassWord, pMqttTunnelItem->szProducerTopic))
							{
								pProducerKafka->m_bConnectFlag = 1;
								if (pProducerKafka->push_data_to_kafka(val.GetBuffer(), val.GetLength(), path.GetBuffer(), path.GetLength()) == -1)
								{
									pProducerKafka->destroy();
									pProducerKafka->m_bConnectFlag = 0;
								}
							}
							else
							{
								pProducerKafka->m_bConnectFlag = 0;
							}
						}
						else
						{
							if (pProducerKafka->push_data_to_kafka(val.GetBuffer(), val.GetLength(), path.GetBuffer(), path.GetLength()) == -1)
							{
								pProducerKafka->destroy();
								pProducerKafka->m_bConnectFlag = 0;
							}
						}
						path.ReleaseBuffer();
						val.ReleaseBuffer();
					}
				}
				//
				resToken = szParseResult.Tokenize(_T("\r\n"), curPos);
			}
			ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
			//
			if (stricmp(pMqttTunnelItem->szType, "ThingsDataLink") == 0)
			{
				if (nReadCount > 0)
				{
					if (UpdateList(pReadBuffer, nReadCount) == -1)
					{
						CTime t1 = CTime::GetCurrentTime();
						CTime t2 = CTime(tValueData.TimeStamp);
						CTimeSpan ts = t1 - t2;
						if (ts.GetTotalSeconds() >= g_OffineSpan)
							SaveList(pReadBuffer, nReadCount);
						//
						LogEvent("%s 写入上行ThingsData变量（%d个）失败！", pMqttTunnelItem->szName,nReadCount);
					}
					else
					{
						LogEvent("%s 写入上行ThingsData变量（%d个）成功！", pMqttTunnelItem->szName, nReadCount);
					}
				}
			}
			//
			if(pReadBuffer)
				free(pReadBuffer);
		}
	}
}

void CMqttTunnel::on_subscribe(int mid, int qos_count, const int *granted_qos)
{
}

int CMqttTunnel::ParseMqttPayload(const char* lpszParseCode, const char* lpszTopic, const char* lpszPayloadType, const char* lpszPayload, int nPayloadLen, CString &lpszParseResult)
{
	if (hSession == NULL
		|| hConnect == NULL)
	{
		hSession = WinHttpOpen(L"ThingsMix-MQTT",
			WINHTTP_ACCESS_TYPE_DEFAULT_PROXY,
			WINHTTP_NO_PROXY_NAME,
			WINHTTP_NO_PROXY_BYPASS, 0);

		if (hSession == NULL)
		{
			LogEvent("ParseMqttPayload WinHttpOpen Error!");
			return -1;
		}
		//
		if (!WinHttpSetTimeouts(hSession,15000, 15000, 15000, 60000))
		{
			//
			LogEvent("ParseMqttPayload WinHttpSetTimeouts Error!");
			//
			if (hSession) WinHttpCloseHandle(hSession);
			//
			hSession = NULL;
			return -1;
		}
		//
		USES_CONVERSION;
		hConnect = WinHttpConnect(hSession, L"127.0.0.1",
			8234+ pMqttTunnelItem->TunnelID%101, 0);
		if (hConnect == NULL)
		{
			LogEvent("ParseMqttPayload WinHttpConnect Error!");
			//
			if (hSession)
				WinHttpCloseHandle(hSession);
			//
			hSession = NULL;
			return -1;
		}
	}
	//
	HINTERNET  hRequest = WinHttpOpenRequest(hConnect, L"POST", L"/srv/ParseMqttPayload.ejs",
		NULL, WINHTTP_NO_REFERER,
		WINHTTP_DEFAULT_ACCEPT_TYPES,
		0);
	if (hRequest == NULL)
	{
		LogEvent("ParseMqttPayload WinHttpOpenRequest Error!");
		//
		if (hConnect)
			WinHttpCloseHandle(hConnect);
		if (hSession)
			WinHttpCloseHandle(hSession);
		//
		hConnect = NULL;
		hSession = NULL;
		return -1;
	}
	//
	int nMaxRed = WINHTTP_DISABLE_REDIRECTS;
	BOOL  bResults = WinHttpSetOption(hRequest,
		WINHTTP_OPTION_DISABLE_FEATURE,
		&nMaxRed,
		sizeof(nMaxRed));
	if (hRequest == NULL)
	{
		LogEvent("ParseMqttPayload WinHttpSetOption  Error!");
		//
		if (hRequest) WinHttpCloseHandle(hRequest);
		if (hConnect) WinHttpCloseHandle(hConnect);
		if (hSession) WinHttpCloseHandle(hSession);
		//
		hRequest = NULL;
		hConnect = NULL;
		hSession = NULL;
		return -1;
	}
	//
	CString szPayload;
	char *pszPostContent = NULL;
	INT	nPostContent=0;
	if (m_MapPayload.Lookup(lpszTopic, szPayload)==0)
	{
		CString CurPayload= URLEncode(ConvertGBKToUtf8(lpszPayload));
		pszPostContent = (char *)malloc(strlen(lpszParseCode) + strlen(lpszTopic) + CurPayload.GetLength() + 64);
		sprintf(pszPostContent, "ParseCode=%s&Topic=%s&PayloadType=%s&Preload=&Payload=%s", lpszParseCode, lpszTopic, lpszPayloadType, CurPayload);
		nPostContent = strlen(pszPostContent);
	}
	else
	{
		CString PrePayload = URLEncode(ConvertGBKToUtf8(szPayload));
		CString CurPayload = URLEncode(ConvertGBKToUtf8(lpszPayload));
		pszPostContent = (char *)malloc(strlen(lpszParseCode) + strlen(lpszTopic) + PrePayload.GetLength() + CurPayload.GetLength() + 64);
		sprintf(pszPostContent, "ParseCode=%s&Topic=%s&PayloadType=%s&Preload=%s&Payload=%s", lpszParseCode, lpszTopic, lpszPayloadType, PrePayload, CurPayload);
		nPostContent = strlen(pszPostContent);
	}
	//
	bResults = WinHttpSendRequest(hRequest,
		WINHTTP_NO_ADDITIONAL_HEADERS,
		0,
		pszPostContent,
		nPostContent,
		nPostContent,
		0);
	if (!bResults)
	{
		LogEvent("ParseMqttPayload WinHttpSendRequest %s Error：%s!", pMqttTunnelItem->szName, FormatMsg(GetLastError()));
		//
		if (pszPostContent)
		{
			free(pszPostContent);
			pszPostContent = NULL;
		}
		//
		if (hRequest) WinHttpCloseHandle(hRequest);
		if (hConnect) WinHttpCloseHandle(hConnect);
		if (hSession) WinHttpCloseHandle(hSession);
		//
		hRequest = NULL;
		hConnect = NULL;
		hSession = NULL;
		//
		m_ParseTryCount++;
		if (m_ParseTryCount > 5)
		{
			LogEvent("ParseMqttPayload WinHttpSendRequest %s ParseTryCount=%ld!", pMqttTunnelItem->szName, m_ParseTryCount);
			m_ParseTryCount = 0;
			return -1;
		}
		Sleep(200);
		return ParseMqttPayload(lpszParseCode,lpszTopic,lpszPayloadType, lpszPayload, nPayloadLen, lpszParseResult);
	}
	//
	m_ParseTryCount = 0;
	if (pszPostContent)
	{
		free(pszPostContent);
		pszPostContent = NULL;
	}
	//
	bResults = WinHttpReceiveResponse(hRequest, NULL);
	if (!bResults)
	{
		LogEvent("ParseMqttPayloadl WinHttpReceiveResponse Error：%s!", FormatMsg(GetLastError()));
		//
		if (hRequest) WinHttpCloseHandle(hRequest);
		if (hConnect) WinHttpCloseHandle(hConnect);
		if (hSession) WinHttpCloseHandle(hSession);
		//
		hRequest = NULL;
		hConnect = NULL;
		hSession = NULL;
		return -1;
	}
	//
	DWORD dwSize = sizeof(DWORD);
	DWORD dwDownloaded = 0;
	DWORD dwStatusCode = 0;

	bResults = WinHttpQueryHeaders(hRequest,
		WINHTTP_QUERY_STATUS_CODE |
		WINHTTP_QUERY_FLAG_NUMBER,
		NULL,
		&dwStatusCode,
		&dwSize,
		NULL);
	if (!bResults)
	{
		LogEvent("ParseMqttPayload WinHttpQueryHeaders Error：%08X!", GetLastError());
		//
		if (hRequest) WinHttpCloseHandle(hRequest);
		if (hConnect) WinHttpCloseHandle(hConnect);
		if (hSession) WinHttpCloseHandle(hSession);
		//
		hRequest = NULL;
		hConnect = NULL;
		hSession = NULL;
		return -1;
	}
	//
	switch (dwStatusCode)
	{
		case 200:
		{
			DWORD dwCount = 0;
			char *pszOutBuffer = NULL;
			do {
				dwSize = 0;
				if (!WinHttpQueryDataAvailable(hRequest, &dwSize)
					|| dwSize == 0)
				{
					break;
				}
				//
				if (!pszOutBuffer)
				{
					pszOutBuffer = (char *)malloc(dwSize + 1);
				}
				else
				{
					pszOutBuffer = (char *)realloc(pszOutBuffer, dwCount + dwSize + 1);
				}
				// Read the Data.
				if (!WinHttpReadData(hRequest, (LPVOID)(pszOutBuffer + dwCount),
					dwSize, &dwDownloaded))
				{
					free(pszOutBuffer);
					pszOutBuffer = NULL;

					if (hRequest) WinHttpCloseHandle(hRequest);
					if (hConnect) WinHttpCloseHandle(hConnect);
					if (hSession) WinHttpCloseHandle(hSession);
					//
					hRequest = NULL;
					hConnect = NULL;
					hSession = NULL;
					return -1;
				}
				else
				{
					dwCount += dwSize;
					pszOutBuffer[dwCount] = '\0';
				}
			} while (dwSize>0);
			//
			if (pszOutBuffer)
			{
				lpszParseResult = ConvertUtf8ToGBK(pszOutBuffer);
				//
				free(pszOutBuffer);
				pszOutBuffer = NULL;
			}
			break;
		}
		default:
		{
			LogEvent("ParseMqttPayload dwStatusCode=%d", dwStatusCode);
			//
			if (hRequest) WinHttpCloseHandle(hRequest);
			//if (hConnect) WinHttpCloseHandle(hConnect);
			//if (hSession) WinHttpCloseHandle(hSession);
			//
			hRequest = NULL;
			//hConnect = NULL;
			//hSession = NULL;
			return -1;
		}
	}
	//
	if (hRequest) WinHttpCloseHandle(hRequest);
	//if (hConnect) WinHttpCloseHandle(hConnect);
	//if (hSession) WinHttpCloseHandle(hSession);
	//
	hRequest = NULL;
	//hConnect = NULL;
	//hSession = NULL;
	return 0;
}

int CMqttTunnel::RegServer(void)
{
	int				rets = 0;
	u_int32_t		uLen;
	u_int32_t		uRes;
	//
	WaitForSingleObject(hSockMutex, INFINITE);
	if (SendSocketCMD(m_ServerSock, TNL_CMD_REGSRV, sizeof(struct TunnelServerItem)) == -1)
	{
		LogEvent("RegServer #1:无法发送数据到MQTT隧道服务端(%s)！", pMqttTunnelItem->szName);
		ReleaseMutex(hSockMutex);
		return -1;
	}
	//
	struct TunnelServerItem info1;
	memset(&info1, 0, sizeof(struct TunnelServerItem));
	sprintf_s(info1.szTunnelName, "%s", pMqttTunnelItem->szName);
	sprintf_s(info1.szTunnelInfo, "%s:%d", pMqttTunnelItem->szMqttSlaveHost, pMqttTunnelItem->nMqttSlavePort);
	sprintf_s(info1.szMachineNO, "%s", g_szMachineNO);
	info1.TunnelID = pMqttTunnelItem->TunnelID;
	//
	if (SendSocketData(m_ServerSock, (char *)&info1, sizeof(info1)) == -1)
	{
		LogEvent("RegServer #4:无法发送数据到MQTT隧道服务端(%s)！", pMqttTunnelItem->szName);
		ReleaseMutex(hSockMutex);
		return -1;
	}
	rets = ReadSocketRES(m_ServerSock, &uRes, &uLen);
	if (rets <= 0)
	{
		LogEvent("RegServer #5:无法读取MQTT隧道服务端命令确认(%s)！", pMqttTunnelItem->szName);
		ReleaseMutex(hSockMutex);
		return -1;
	}
	if (uRes != NET_RES_OK)
	{
		LogEvent("RegServer #6:向平台注册MQTT隧道服务失败(%s)！", pMqttTunnelItem->szName);
		ReleaseMutex(hSockMutex);
		return -1;
	}
	//
	ReleaseMutex(hSockMutex);
	//历史缓存数据
	PlayItem();
	//
	int nRegCount = 0;
	int nRegTotal = 0;
	char *pRegBuffer = (char *)malloc(sizeof(struct TunnelValueItem) * 64);
	//
	WaitForSingleObject(pMqttTunnelItem->hReadOPCMutex, INFINITE);
	for (int j = 0; j<pMqttTunnelItem->list.size(); j++)
	{
		if (pMqttTunnelItem->list[j].quality== OPC_QUALITY_GOOD)
		{
			CString szDataType = _T("");
			CString szDataValue = _T("");
			//
			struct TunnelValueItem info1;
			memset(&info1, 0, sizeof(struct TunnelValueItem));
			info1.id = pMqttTunnelItem->list[j].id;
			sprintf_s(info1.no, "%s", pMqttTunnelItem->list[j].no);
			sprintf_s(info1.szOPCPath, "%s", pMqttTunnelItem->list[j].szDstOPCPath);
			info1.TunnelID = pMqttTunnelItem->TunnelID;
			info1.TimeStamp = pMqttTunnelItem->list[j].dataTime;
			info1.ValueType = VT_R8;
			info1.dblValue = pMqttTunnelItem->list[j].dblValue;
			memcpy(pRegBuffer + sizeof(struct TunnelValueItem)*nRegCount, &info1, sizeof(struct TunnelValueItem));
			nRegCount++;
			//
			_Module.m_UpdateTime = CTime::GetCurrentTime();
		}
		//
		if (nRegCount >= 64)
		{
			nRegTotal += nRegCount;
			nRegCount = 0;
			//
			WaitForSingleObject(hSockMutex, INFINITE);
			if (SendSocketCMD(m_ServerSock, TNL_CMD_REGVAL, sizeof(struct TunnelValueItem) * 64) == -1)
			{
				free(pRegBuffer);
				//
				LogEvent("RegOPCServer #7:无法发送命令到MQTT隧道服务端(%s)！", pMqttTunnelItem->szName);
				ReleaseMutex(hSockMutex);
				ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
				return -1;
			}
			//
			if (SendSocketData(m_ServerSock, pRegBuffer, sizeof(struct TunnelValueItem) * 64) == -1)
			{
				free(pRegBuffer);
				//
				LogEvent("RegOPCServer #8:无法发送数据到MQTT隧道服务端(%s)！", pMqttTunnelItem->szName);
				ReleaseMutex(hSockMutex);
				ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
				return -1;
			}
			rets = ReadSocketRES(m_ServerSock, &uRes, &uLen);
			if (rets <= 0)
			{
				free(pRegBuffer);
				//
				LogEvent("RegOPCServer #9:无法读取命令从MQTT隧道服务端(%s)！", pMqttTunnelItem->szName);
				ReleaseMutex(hSockMutex);
				ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
				return -1;
			}
			if (uRes != NET_RES_OK)
			{
				free(pRegBuffer);
				//
				LogEvent("RegOPCServer #10:MQTT隧道服务端(%s)注册变量失败！", pMqttTunnelItem->szName);
				ReleaseMutex(hSockMutex);
				ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
				return -1;
			}
			LogEvent("已向MQTT隧道服务端(%s)注册原始变量成功（%d）个!", pMqttTunnelItem->szName, nRegTotal);
			ReleaseMutex(hSockMutex);
		}
	}
	//
	if (nRegCount >0)
	{
		WaitForSingleObject(hSockMutex, INFINITE);
		if (SendSocketCMD(m_ServerSock, TNL_CMD_REGVAL, sizeof(struct TunnelValueItem) * nRegCount) == -1)
		{
			free(pRegBuffer);
			//
			LogEvent("RegServer #7:无法发送命令到OPC隧道服务端(%s)！", pMqttTunnelItem->szName);
			ReleaseMutex(hSockMutex);
			ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
			return -1;
		}
		//
		if (SendSocketData(m_ServerSock, pRegBuffer, sizeof(struct TunnelValueItem) * nRegCount) == -1)
		{
			free(pRegBuffer);
			//
			LogEvent("RegServer #8:无法发送数据到MQTT隧道服务端(%s)！", pMqttTunnelItem->szName);
			ReleaseMutex(hSockMutex);
			ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
			return -1;
		}
		rets = ReadSocketRES(m_ServerSock, &uRes, &uLen);
		if (rets <= 0)
		{
			free(pRegBuffer);
			//
			LogEvent("RegServer #9:无法读取命令从MQTT隧道服务端(%s)！", pMqttTunnelItem->szName);
			ReleaseMutex(hSockMutex);
			ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
			return -1;
		}
		if (uRes != NET_RES_OK)
		{
			free(pRegBuffer);
			//
			LogEvent("RegServer #10:MQTT隧道服务端(%s)注册变量失败！", pMqttTunnelItem->szName);
			ReleaseMutex(hSockMutex);
			ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
			return -1;
		}
		LogEvent("已向MQTT隧道服务端(%s)注册原始变量成功（%d）个!", pMqttTunnelItem->szName, nRegTotal + nRegCount);
		ReleaseMutex(hSockMutex);
	}
	ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
	free(pRegBuffer);
	return 0;
}

int CMqttTunnel::HeatServer(void)
{
	int				rets = 0;
	u_int32_t		uLen;
	u_int32_t		uRes;
	//
	WaitForSingleObject(hSockMutex, INFINITE);
	if (SendSocketCMD(m_ServerSock, TNL_CMD_HEAT, 0) == -1)
	{
		LogEvent("无法发送心跳到MQTT隧道服务端(%s)！", pMqttTunnelItem->szName);
		ReleaseMutex(hSockMutex);
		return -1;
	}
	rets = ReadSocketRES(m_ServerSock, &uRes, &uLen);
	if (rets <= 0)
	{
		LogEvent("#1无法读取MQTT隧道服务端命令确认(%s)！", pMqttTunnelItem->szName);
		ReleaseMutex(hSockMutex);
		return -1;
	}
	if (uRes != NET_RES_OK)
	{
		LogEvent("向平台心跳MQTT隧道服务失败(%s)！", pMqttTunnelItem->szName);
		ReleaseMutex(hSockMutex);
		return -1;
	}
	_Module.m_UpdateTime = CTime::GetCurrentTime();
	ReleaseMutex(hSockMutex);
	//
	return 0;
}


void CMqttTunnel::SaveLast()
{
	char  szDataFile[2048];
	sprintf(szDataFile, "%sappdata/%s.last", g_ServricePath, m_szMqttID);
	FILE* pf = fopen(szDataFile, "wb");
	if (pf != NULL)
	{
		CTime t1 = CTime::GetCurrentTime();
		WaitForSingleObject(pMqttTunnelItem->hReadOPCMutex, INFINITE);
		for (int j = 0; j < pMqttTunnelItem->list.size(); j++)
		{
			CTime t2 = CTime(pMqttTunnelItem->list[j].dataTime);
			CTimeSpan ts = t1 - t2;
			if (ts.GetTotalSeconds() < g_ActiveTime * 3)
			{
				struct TunnelValueItem info1;
				memset(&info1, 0, sizeof(struct TunnelValueItem));
				info1.id = pMqttTunnelItem->list[j].id;
				sprintf_s(info1.no, "%s", pMqttTunnelItem->list[j].no);
				sprintf_s(info1.szOPCPath, "%s", pMqttTunnelItem->list[j].szDstOPCPath);
				info1.TunnelID = pMqttTunnelItem->TunnelID;
				info1.TimeStamp = pMqttTunnelItem->list[j].dataTime;
				info1.ValueType = VT_R8;
				info1.dblValue = pMqttTunnelItem->list[j].dblValue;
				fwrite((const  void*)&info1, sizeof(struct TunnelValueItem), 1, pf);
			}
		}
		ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
		fclose(pf);
	}
}

void CMqttTunnel::PlayLast()
{
	char  szDataFile[8192];
	sprintf(szDataFile, "%sappdata/%s.last", g_ServricePath, m_szMqttID);
	FILE* pf3 = fopen(szDataFile, "rb");
	if (pf3 != NULL)
	{
		int nTotal = 0;
		char *pReadBuffer = (char *)malloc(sizeof(struct TunnelValueItem) * 64);
		//
		while (!feof(pf3))
		{
			int nCount = fread(pReadBuffer, sizeof(struct TunnelValueItem), 64, pf3);
			if (nCount>0 && nCount <= 64)
			{
				//恢复最后记录
				for (int i = 0; i <nCount; i++)
				{
					struct TunnelValueItem *pValue = (struct TunnelValueItem *)(pReadBuffer + i * sizeof(struct TunnelValueItem));
					struct MqttToOPCValueItem	*pItem = NULL;
					if (m_MqttMapTable3.Lookup(pValue->id, pItem) != 0
						&& pItem->quality == OPC_QUALITY_BAD)
					{
						pItem->dblValue = pValue->dblValue;
						pItem->dataTime = pValue->TimeStamp;
					}
				}
				//
				int				rets = 0;
				u_int32_t		uLen;
				u_int32_t		uRes;
				WaitForSingleObject(hSockMutex, INFINITE);
				if (SendSocketCMD(m_ServerSock, TNL_CMD_UPDATE, sizeof(struct TunnelValueItem)*nCount) == -1)
				{
					LogEvent("无法发送数据到隧道服务端2(%s)！", pMqttTunnelItem->szName);
					ReleaseMutex(hSockMutex);
					free(pReadBuffer);
					fclose(pf3);
					return;
				}
				//
				if (SendSocketData(m_ServerSock, pReadBuffer, sizeof(struct TunnelValueItem)*nCount) == -1)
				{
					LogEvent("无法发送数据到隧道服务端1(%s)！", pMqttTunnelItem->szName);
					ReleaseMutex(hSockMutex);
					free(pReadBuffer);
					fclose(pf3);
					return;
				}
				rets = ReadSocketRES(m_ServerSock, &uRes, &uLen);
				if (rets <= 0)
				{
					LogEvent("#2无法读取隧道服务端命令确认(%s)！", pMqttTunnelItem->szName);
					ReleaseMutex(hSockMutex);
					free(pReadBuffer);
					fclose(pf3);
					return;
				}
				if (uRes != NET_RES_OK)
				{
					LogEvent("隧道服务向平台更新变量失败(%s)！", pMqttTunnelItem->szName);
					ReleaseMutex(hSockMutex);
					free(pReadBuffer);
					fclose(pf3);
					return;
				}
				ReleaseMutex(hSockMutex);
				//
				nTotal += nCount;
				LogEvent("隧道服务（%s）向平台续传历史变量成功%d个！", pMqttTunnelItem->szName, nTotal);
			}
			else
			{
				LogEvent("隧道服务（%s）向平台读取历史变量失败%d个！", pMqttTunnelItem->szName, nCount);
			}
		}
		//
		free(pReadBuffer);
		fclose(pf3);
	}
}


void CMqttTunnel::ReadCSVFile(DWORD TunnelID)
{
	CStdioFile hCSVFile;
	char  szCSVFile[8192];
	sprintf_s(szCSVFile, 8192, "%sappdata\\%lu.csv", g_ServricePath, TunnelID);
	if (hCSVFile.Open(szCSVFile, CFile::shareDenyNone))
	{
		int nRow = 0;
		CString szRowValue;
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
				char buff[8][1024];
				int i = 0;
				for (i = 0; i<8; i++)
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
				CString szKey = "";
				szKey.Format("%lu/%s", TunnelID, buff[0]);
				m_MapTopicValue[szKey] = atof(buff[2]);
			}
		}
		hCSVFile.Close();
	}
	else
	{
		WriteCSVFile(TunnelID, TRUE, "变量主题,数据时间,数据值");
	}
}
void CMqttTunnel::PlayItem()
{
	char  szDataFile[8192];
	sprintf(szDataFile, "%sappdata/%s.bak", g_ServricePath, m_szMqttID);
	FILE* pf1 = fopen(szDataFile, "rb");
	if (pf1 != NULL)
	{
		int nTotal = 0;
		char *pReadBuffer = (char *)malloc(sizeof(struct TunnelValueItem) * 64);
		//
		while (!feof(pf1))
		{
			struct TunnelValueItem item;
			int nCount = fread(&item, sizeof(struct TunnelValueItem), 1, pf1);
			if (nCount > 0)
			{
				FILETIME nHisTimeStamp;
				if (m_HisTimeStampMap.Lookup(item.id, nHisTimeStamp) == 0)
				{
					memcpy(pReadBuffer + sizeof(struct TunnelValueItem)*nTotal, &item, sizeof(struct TunnelValueItem));
					m_HisTimeStampMap.SetAt(item.id, item.TimeStamp);
					nTotal++;
				}
				else
				{
					CTime t1 = CTime(nHisTimeStamp);
					CTime t2 = CTime(item.TimeStamp);
					if (t2 > t1)
					{
						memcpy(pReadBuffer + sizeof(struct TunnelValueItem)*nTotal, &item, sizeof(struct TunnelValueItem));
						m_HisTimeStampMap.SetAt(item.id, item.TimeStamp);
						nTotal++;
					}
				}
			}
			//
			if (nTotal == 64)
			{
				int				rets = 0;
				u_int32_t		uLen;
				u_int32_t		uRes;
				WaitForSingleObject(hSockMutex, INFINITE);
				if (SendSocketCMD(m_ServerSock, TNL_CMD_UPDATE, sizeof(struct TunnelValueItem)*nTotal) == -1)
				{
					LogEvent("无法发送数据到隧道服务端2(%s)！", pMqttTunnelItem->szName);
					ReleaseMutex(hSockMutex);
					free(pReadBuffer);
					fclose(pf1);
					return;
				}
				//
				if (SendSocketData(m_ServerSock, pReadBuffer, sizeof(struct TunnelValueItem)*nTotal) == -1)
				{
					LogEvent("无法发送数据到隧道服务端1(%s)！", pMqttTunnelItem->szName);
					ReleaseMutex(hSockMutex);
					free(pReadBuffer);
					fclose(pf1);
					return;
				}
				rets = ReadSocketRES(m_ServerSock, &uRes, &uLen);
				if (rets <= 0)
				{
					LogEvent("#3无法读取隧道服务端命令确认(%s)！", pMqttTunnelItem->szName);
					ReleaseMutex(hSockMutex);
					free(pReadBuffer);
					fclose(pf1);
					return;
				}
				if (uRes != NET_RES_OK)
				{
					LogEvent("隧道服务向平台更新变量失败(%s)！", pMqttTunnelItem->szName);
					ReleaseMutex(hSockMutex);
					free(pReadBuffer);
					fclose(pf1);
					return;
				}
				ReleaseMutex(hSockMutex);
				//
				LogEvent("隧道服务（%s）向平台续传历史变量成功%d个！", pMqttTunnelItem->szName, nTotal);
				nTotal = 0;
			}
		}
		//
		if (nTotal>0)
		{
			int				rets = 0;
			u_int32_t		uLen;
			u_int32_t		uRes;
			WaitForSingleObject(hSockMutex, INFINITE);
			if (SendSocketCMD(m_ServerSock, TNL_CMD_UPDATE, sizeof(struct TunnelValueItem)*nTotal) == -1)
			{
				LogEvent("无法发送数据到隧道服务端2(%s)！", pMqttTunnelItem->szName);
				ReleaseMutex(hSockMutex);
				free(pReadBuffer);
				fclose(pf1);
				return;
			}
			//
			if (SendSocketData(m_ServerSock, pReadBuffer, sizeof(struct TunnelValueItem)*nTotal) == -1)
			{
				LogEvent("无法发送数据到隧道服务端1(%s)！", pMqttTunnelItem->szName);
				ReleaseMutex(hSockMutex);
				free(pReadBuffer);
				fclose(pf1);
				return;
			}
			rets = ReadSocketRES(m_ServerSock, &uRes, &uLen);
			if (rets <= 0)
			{
				LogEvent("#4无法读取隧道服务端命令确认(%s)！", pMqttTunnelItem->szName);
				ReleaseMutex(hSockMutex);
				free(pReadBuffer);
				fclose(pf1);
				return;
			}
			if (uRes != NET_RES_OK)
			{
				LogEvent("隧道服务向平台更新变量失败(%s)！", pMqttTunnelItem->szName);
				ReleaseMutex(hSockMutex);
				free(pReadBuffer);
				fclose(pf1);
				return;
			}
			ReleaseMutex(hSockMutex);
			//
			LogEvent("隧道服务（%s）向平台续传历史变量成功%d个！", pMqttTunnelItem->szName, nTotal);
			nTotal = 0;
		}
		//
		free(pReadBuffer);
		fclose(pf1);
		//
		DeleteFile(szDataFile);
	}
	sprintf(szDataFile, "%sappdata/%s.db", g_ServricePath, m_szMqttID);
	FILE* pf2 = fopen(szDataFile, "rb");
	if (pf2 != NULL)
	{
		int nTotal = 0;
		char *pReadBuffer = (char *)malloc(sizeof(struct TunnelValueItem) * 64);
		//
		while (!feof(pf2))
		{
			struct TunnelValueItem item;
			int nCount = fread(&item, sizeof(struct TunnelValueItem), 1, pf2);
			if (nCount > 0)
			{
				FILETIME nHisTimeStamp;
				if (m_HisTimeStampMap.Lookup(item.id, nHisTimeStamp) == 0)
				{
					memcpy(pReadBuffer + sizeof(struct TunnelValueItem)*nTotal, &item, sizeof(struct TunnelValueItem));
					m_HisTimeStampMap.SetAt(item.id, item.TimeStamp);
					nTotal++;
				}
				else
				{
					CTime t1 = CTime(nHisTimeStamp);
					CTime t2 = CTime(item.TimeStamp);
					if (t2 > t1)
					{
						memcpy(pReadBuffer + sizeof(struct TunnelValueItem)*nTotal, &item, sizeof(struct TunnelValueItem));
						m_HisTimeStampMap.SetAt(item.id, item.TimeStamp);
						nTotal++;
					}
				}
			}
			//
			if (nTotal == 64)
			{
				int				rets = 0;
				u_int32_t		uLen;
				u_int32_t		uRes;
				WaitForSingleObject(hSockMutex, INFINITE);
				if (SendSocketCMD(m_ServerSock, TNL_CMD_UPDATE, sizeof(struct TunnelValueItem)*nTotal) == -1)
				{
					LogEvent("无法发送数据到隧道服务端2(%s)！", pMqttTunnelItem->szName);
					ReleaseMutex(hSockMutex);
					free(pReadBuffer);
					fclose(pf2);
					return;
				}
				//
				if (SendSocketData(m_ServerSock, pReadBuffer, sizeof(struct TunnelValueItem)*nTotal) == -1)
				{
					LogEvent("无法发送数据到隧道服务端1(%s)！", pMqttTunnelItem->szName);
					ReleaseMutex(hSockMutex);
					free(pReadBuffer);
					fclose(pf2);
					return;
				}
				rets = ReadSocketRES(m_ServerSock, &uRes, &uLen);
				if (rets <= 0)
				{
					LogEvent("#6无法读取隧道服务端命令确认(%s)！", pMqttTunnelItem->szName);
					ReleaseMutex(hSockMutex);
					free(pReadBuffer);
					fclose(pf2);
					return;
				}
				if (uRes != NET_RES_OK)
				{
					LogEvent("隧道服务向平台更新变量失败(%s)！", pMqttTunnelItem->szName);
					ReleaseMutex(hSockMutex);
					free(pReadBuffer);
					fclose(pf2);
					return;
				}
				ReleaseMutex(hSockMutex);
				//
				LogEvent("隧道服务（%s）向平台续传历史变量成功%d个！", pMqttTunnelItem->szName, nTotal);
				nTotal = 0;
			}
		}
		//
		if (nTotal>0)
		{
			int				rets = 0;
			u_int32_t		uLen;
			u_int32_t		uRes;
			WaitForSingleObject(hSockMutex, INFINITE);
			if (SendSocketCMD(m_ServerSock, TNL_CMD_UPDATE, sizeof(struct TunnelValueItem)*nTotal) == -1)
			{
				LogEvent("无法发送数据到隧道服务端2(%s)！", pMqttTunnelItem->szName);
				ReleaseMutex(hSockMutex);
				free(pReadBuffer);
				fclose(pf2);
				return;
			}
			//
			if (SendSocketData(m_ServerSock, pReadBuffer, sizeof(struct TunnelValueItem)*nTotal) == -1)
			{
				LogEvent("无法发送数据到隧道服务端1(%s)！", pMqttTunnelItem->szName);
				ReleaseMutex(hSockMutex);
				free(pReadBuffer);
				fclose(pf2);
				return;
			}
			rets = ReadSocketRES(m_ServerSock, &uRes, &uLen);
			if (rets <= 0)
			{
				LogEvent("#7无法读取隧道服务端命令确认(%s)！", pMqttTunnelItem->szName);
				ReleaseMutex(hSockMutex);
				free(pReadBuffer);
				fclose(pf2);
				return;
			}
			if (uRes != NET_RES_OK)
			{
				LogEvent("隧道服务向平台更新变量失败(%s)！", pMqttTunnelItem->szName);
				ReleaseMutex(hSockMutex);
				free(pReadBuffer);
				fclose(pf2);
				return;
			}
			ReleaseMutex(hSockMutex);
			//
			LogEvent("隧道服务（%s）向平台续传历史变量成功%d个！", pMqttTunnelItem->szName, nTotal);
			nTotal = 0;
		}
		//
		free(pReadBuffer);
		fclose(pf2);
		//
		DeleteFile(szDataFile);
	}
	bHisFlag = 0;
	nHisCount = 0;
}

void CMqttTunnel::SaveItem(struct TunnelValueItem *pValue)
{
	char  szDataFile[2048];
	char  szBackupFile[2048];
	if (nHisCount >= g_OffineRecord)
	{
		sprintf(szDataFile, "%sappdata/%s.db", g_ServricePath, m_szMqttID);
		sprintf(szBackupFile, "%sappdata/%s.bak", g_ServricePath, m_szMqttID);
		DeleteFile(szBackupFile);
		MoveFile(szDataFile, szBackupFile);
		nHisCount = 0;
	}
	else
	{
		sprintf(szDataFile, "%sappdata/%s.db", g_ServricePath, m_szMqttID);
	}
	//
	FILE* pf = fopen(szDataFile, "ab");
	if (pf != NULL)
	{
		bHisFlag = 1;
		nHisCount++;
		fwrite((const  void*)pValue, sizeof(struct TunnelValueItem), 1, pf);
		fclose(pf);
	}
}

int CMqttTunnel::UpdateList(char *pReadBuffer, int nCount)
{
	if (m_ServerSock != INVALID_SOCKET)
	{
		//历史缓存数据
		if (bHisFlag == 1)
			PlayItem();
		//
		int				rets = 0;
		u_int32_t		uLen;
		u_int32_t		uRes;
		WaitForSingleObject(hSockMutex, INFINITE);
		if (SendSocketCMD(m_ServerSock, TNL_CMD_UPDATE, sizeof(struct TunnelValueItem)*nCount) == -1)
		{
			LogEvent("无法发送数据到隧道服务端2(%s)！", pMqttTunnelItem->szName);
			ReleaseMutex(hSockMutex);
			return -1;
		}
		//
		if (SendSocketData(m_ServerSock, pReadBuffer, sizeof(struct TunnelValueItem)*nCount) == -1)
		{
			LogEvent("无法发送数据到隧道服务端1(%s)！", pMqttTunnelItem->szName);
			ReleaseMutex(hSockMutex);
			return -1;
		}
		rets = ReadSocketRES(m_ServerSock, &uRes, &uLen);
		if (rets <= 0)
		{
			LogEvent("#8无法读取隧道服务端命令确认(%s)！", pMqttTunnelItem->szName);
			ReleaseMutex(hSockMutex);
			return -1;
		}
		if (uRes != NET_RES_OK)
		{
			LogEvent("隧道服务向平台更新变量失败(%s)！", pMqttTunnelItem->szName);
			ReleaseMutex(hSockMutex);
			return -1;
		}
		ReleaseMutex(hSockMutex);
		return 0;
	}
	return -1;
}

void CMqttTunnel::SaveList(char *pReadBuffer, int nCount)
{
	char  szDataFile[2048];
	char  szBackupFile[2048];
	if (nHisCount >= g_OffineRecord)
	{
		sprintf(szDataFile, "%sappdata/%s.db", g_ServricePath, m_szMqttID);
		sprintf(szBackupFile, "%sappdata/%s.bak", g_ServricePath, m_szMqttID);
		DeleteFile(szBackupFile);
		MoveFile(szDataFile, szBackupFile);
		nHisCount = 0;
	}
	else
	{
		sprintf(szDataFile, "%sappdata/%s.db", g_ServricePath, m_szMqttID);
	}
	//
	FILE* pf = fopen(szDataFile, "ab");
	if (pf != NULL)
	{
		bHisFlag = 1;
		nHisCount += nCount;
		fwrite((const  void*)pReadBuffer, sizeof(struct TunnelValueItem)*nCount, 1, pf);
		fclose(pf);
	}
}

int CMqttTunnel::UpdateItem(struct TunnelValueItem *pValue)
{
	if (m_ServerSock != INVALID_SOCKET)
	{
		//历史缓存数据
		if (bHisFlag == 1)
			PlayItem();
		//
		int				rets = 0;
		u_int32_t		uLen;
		u_int32_t		uRes;
		WaitForSingleObject(hSockMutex, INFINITE);
		if (SendSocketCMD(m_ServerSock, TNL_CMD_UPDATE, sizeof(struct TunnelValueItem)) == -1)
		{
			LogEvent("UpdateItem #1:无法发送数据到MQTT隧道服务端2(%s)！", m_szIPAddr);
			ReleaseMutex(hSockMutex);
			return -1;
		}
		//
		if (SendSocketData(m_ServerSock, (char *)pValue, sizeof(struct TunnelValueItem)) == -1)
		{
			LogEvent("UpdateItem #11:无法发送数据到MQTT隧道服务端1(%s)！", m_szIPAddr);
			ReleaseMutex(hSockMutex);
			return -1;
		}
		rets = ReadSocketRES(m_ServerSock, &uRes, &uLen);
		if (rets <= 0)
		{
			LogEvent("UpdateItem #22:无法读取MQTT隧道服务端命令确认(%s)！", m_szIPAddr);
			ReleaseMutex(hSockMutex);
			return -1;
		}
		if (uRes != NET_RES_OK)
		{
			LogEvent("UpdateItem #33:向平台注册MQTT隧道服务失败(%s)！", m_szIPAddr);
			ReleaseMutex(hSockMutex);
			return -1;
		}
		ReleaseMutex(hSockMutex);
		//
		LogEvent("映射变量（%s=%f）成功！", pValue->szOPCPath, pValue->dblValue);
		//
		_Module.m_UpdateTime = CTime::GetCurrentTime();
		return 0;
	}
	else
	{
		//保存历史数据
		CTime t1 = CTime::GetCurrentTime();
		CTime t2 = CTime(pValue->TimeStamp);
		CTimeSpan ts = t1 - t2;
		if (ts.GetTotalSeconds() >= g_OffineSpan)
			SaveItem(pValue);
	}
	//
	return 0;
}

int CMqttTunnel::WriteItem(struct TunnelValueItem *pValue)
{
	struct MqttToOPCValueItem	*pItem = NULL;
	if (m_MqttMapTable3.Lookup(pValue->id, pItem) == 0)
	{
		CString szDstTopic = pValue->szOPCPath;
		szDstTopic.Replace('.', '/');
		szDstTopic.Format("%s/%s", pValue->no, szDstTopic.Mid(szDstTopic.ReverseFind('/') + 1));
		if (m_MqttMapTable4.Lookup(szDstTopic, pItem) == 0)
		{
			if (m_MqttMapTable2.Lookup(pValue->szOPCPath, pItem) == 0)
			{
				LogEvent("%s 变量未获取到 #3！", pValue->szOPCPath);
				return -1;
			}
		}
	}
	//
	if (stricmp(pMqttTunnelItem->szMqttSlaveFormat,"TVU")==0)
	{
		CTime tc(pValue->TimeStamp);
		//
		CString szPayload = "";
		szPayload.Format("%s|%s|%s", tc.Format("%Y-%m-%dT%H:%M:%S"), FormatValueType((pValue->dblValue - pItem->fDlt) / pItem->fDiv, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
		//
		pItem->bUpdateFlag = 0;
		if (stricmp(pMqttTunnelItem->szType, "ThingsDataLink") == 0)
		{
			if (strlen(pMqttTunnelItem->szMasteHost) > 0)
			{
				pItem->bUpdateFlag++;
			}
			if (strlen(pMqttTunnelItem->szSlaveHost) > 0)
			{
				pItem->bUpdateFlag++;
			}
		}
		else
		{
			pItem->bUpdateFlag++;
		}
		LogEvent("【下行写入】#4 topic=%s，payload=%s", pItem->szSrcTopic, szPayload);
		//
		if (stricmp(pMqttTunnelItem->szMqttSlaveCode, "UTF8") == 0)
			szPayload = ConvertGBKToUtf8(szPayload);
		else if (stricmp(pMqttTunnelItem->szMqttSlaveCode, "Unicode") == 0)
			szPayload = ConvertGBKToUtf16(szPayload);
		//
		CString szTopic = "";
		if(m_MapTopicSrc.Lookup(pItem->szSrcTopic,szTopic)==0)
			publish(NULL, pItem->szSrcTopic, szPayload.GetLength(), szPayload, 2);
		else
			publish(NULL, szTopic, szPayload.GetLength(), szPayload, 2);
	}
	else
	{
		CString szPayload = ""; 
		CTime tc(pItem->dataTime);
		szPayload.Format("{\"DeviceID\":\"%s\",\"ValueTag\":\"%s\",\"DataTime\":\"%s\",\"DataValue\":%s,\"DataUnit\":\"%s\"}", pItem->no, pItem->szDstOPCPath,tc.Format("%Y-%m-%d %H:%M:%S"), FormatValueType((pValue->dblValue - pItem->fDlt) / pItem->fDiv, pItem->szDispType, pItem->nDispDiv), pItem->szUnit);
		//
		CString szWriteResult = "";
		if (ParseMqttPayload(pMqttTunnelItem->szMqttWriteParseCode, pItem->szSrcTopic, "Publish", szPayload, szPayload.GetLength(), szWriteResult) == -1)
		{
			LogEvent("写入下行MQTT变量（%s）失败，数据解析不正确(%s)！", pItem->szSrcTopic, szPayload);
			return -1;
		}
		//
		pItem->bUpdateFlag = 0;
		if (stricmp(pMqttTunnelItem->szType, "ThingsDataLink") == 0)
		{
			if (strlen(pMqttTunnelItem->szMasteHost) > 0)
			{
				pItem->bUpdateFlag++;
			}
			if (strlen(pMqttTunnelItem->szSlaveHost) > 0)
			{
				pItem->bUpdateFlag++;
			}
		}
		else
		{
			pItem->bUpdateFlag++;
		}
		//
		if (stricmp(pMqttTunnelItem->szMqttSlaveCode, "UTF8") == 0)
			szWriteResult = ConvertGBKToUtf8(szWriteResult);
		else if (stricmp(pMqttTunnelItem->szMqttSlaveCode, "Unicode") == 0)
			szWriteResult = ConvertGBKToUtf16(szWriteResult);
		//
		int offset = szWriteResult.Find("|");
		if (offset == -1)
		{
			LogEvent("【下行写入】#5 topic=%s，payload=%s", pItem->szSrcTopic, szWriteResult);
			CString szTopic = "";
			if (m_MapTopicSrc.Lookup(pItem->szSrcTopic, szTopic) == 0)
				publish(NULL, pItem->szSrcTopic, szWriteResult.GetLength(), szWriteResult,2);
			else
				publish(NULL, szTopic, szPayload.GetLength(), szPayload, 2);
		}
		else
		{
			LogEvent("【下行写入】#6 topic=%s，payload=%s", szWriteResult.Mid(0, offset), szWriteResult.Mid(offset + 1));
			publish(NULL, szWriteResult.Mid(0, offset), szWriteResult.GetLength() - offset - 1, szWriteResult.Mid(offset + 1),2);
		}
	}
	//
	struct MapValueItem myValue;
	memset(&myValue, 0, sizeof(struct MapValueItem));
	myValue.TunnelID = pMqttTunnelItem->TunnelID;
	myValue.ValueID = pItem->id;
	myValue.nUpFlag = 2;
	myValue.nDataType = VT_R8;
	myValue.fDataValue = pItem->dblValue;
	myValue.nDataTime = pItem->dataTime;
	//
	CString szDstOPCPath = pItem->szDstOPCPath;
	int offset = szDstOPCPath.ReverseFind('.');
	if (offset > 0)
	{
		sprintf_s(myValue.szNodePath, "%s", szDstOPCPath.Mid(0, offset));
	}
	//
	SendDataToMeter(&myValue);
	return 0;
}

int CMqttTunnel::ReadItem(const char *path)
{
	WaitForSingleObject(pMqttTunnelItem->hReadOPCMutex, INFINITE);
	for (int j = 0; j<pMqttTunnelItem->list.size(); j++)
	{
		if (MatchingString(pMqttTunnelItem->list[j].szDstOPCPath, path))
		{
			pMqttTunnelItem->list[j].quality = OPC_QUALITY_BAD;
		}
	}
	//
	ReleaseMutex(pMqttTunnelItem->hReadOPCMutex);
	return 0;
}


void CMqttTunnel::SendDataToMeter(struct MapValueItem *pValue)
{
	if (sMeter == INVALID_SOCKET)
	{
		sMeter = ::socket(PF_INET, SOCK_DGRAM, 0);
		//
		if (sMeter == SOCKET_ERROR)
		{
			LogEvent("SendDataToMeter:无法创建Socket服务！");
			return;
		}
		//
		int timeout = 3000; //ms
		setsockopt(sMeter, SOL_SOCKET, SO_RCVTIMEO, (char*)&timeout, sizeof(timeout));
	}

	SOCKADDR_IN			local;
	local.sin_addr.s_addr = inet_addr("127.0.0.1");
	local.sin_family = AF_INET;
	local.sin_port = htons(10000);
	//
	struct CmdHdr rp;
	rp.uCmd = METER_CMD_VALUE;
	rp.uLen = sizeof(struct MapValueItem);
	rp.uMagic = NET_CMD_MAGIC;
	//
	if (sendto(sMeter, (char *)&rp, sizeof(struct CmdHdr), 0, (struct sockaddr*)&local, sizeof(local)) == -1)
	{
		LogEvent("SendDataToMeter:发送监控命令错误！");
		closesocket(sMeter);
		sMeter = INVALID_SOCKET;
		return;
	}
	//
	if (sendto(sMeter, (char *)pValue, sizeof(struct MapValueItem), 0, (struct sockaddr*)&local, sizeof(local)) == -1)
	{
		LogEvent("SendDataToMeter:发送监控数据错误！");
		closesocket(sMeter);
		sMeter = INVALID_SOCKET;
		return;
	}
}
