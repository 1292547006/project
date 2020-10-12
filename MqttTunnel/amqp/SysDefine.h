#ifndef	SYS_DEF_H
#define	SYS_DEF_H

	#if _MSC_VER >= 1000
		#pragma once
	#endif // _MSC_VER >= 1000

	#if defined(WIN32)
		#pragma warning(disable: 4251)
		#pragma warning(disable: 4275)
	#endif

	#if !defined(TRUE)
		#define	TRUE					1
	#endif

	#if !defined(FALSE)
		#define	FALSE					0
	#endif


	#if defined(WIN32)
		#define	STDCALL					__stdcall

		 #define IMEXPORTS  

		#ifdef IMEXPORTS
			#define  Publish __declspec(dllexport)
		#else
			#define  Publish __declspec(dllimport)
		#endif
	#endif

    # if  defined(__linux)
      #define  Publish 
    #endif
/*-------------------------------------------------------------------------*/

	#define CODE_LEN				24
	#define DESC_LEN				40
	#define GRAP_LEN				40
	#define	PASSWORD_LEN			24
	#define	USER_NAME_LEN			CODE_LEN
	#define	RTDB_NAME_LEN			CODE_LEN
	#define HOSTNAMELEN				CODE_LEN
	#define SHORTNAME_LEN			CODE_LEN
	#define FULLDESCRIBE_LEN		128
	#define REASONINFO_LEN			64

/*-------------------------------------------------------------------------*/

#endif			//all of end
