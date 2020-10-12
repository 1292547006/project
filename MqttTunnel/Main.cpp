// Main.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "Service.h"

int main(int argc, char* argv[])
{
	_Module.Init("MqttTunnel","Things MQTT Tunnel");
	if (argc == 1)
	{
		_Module.m_bService = TRUE;
		_Module.Start(NULL, 0);
	}
	else if (argc == 2)
	{
		char seps[] = "-/";
		char *pToken;
		char *pNextToken;
		
		pToken = strtok_s(argv[1],seps,&pNextToken);
		while (pToken)
		{
			if (!_stricmp(pToken,"Install"))
			{
				_Module.Install();
			}
			else if (!_stricmp(pToken,"Uninstall"))
			{
				_Module.Uninstall();
			}
			pToken = strtok_s( NULL, seps ,&pNextToken);
			return 0;
		}
	}
	else if (argc == 3)
	{
		_Module.m_bService = FALSE;
		_Module.Start(argv[1], strtoul(argv[2], NULL, 10));
	}

	return 0;
}
