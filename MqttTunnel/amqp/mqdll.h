#ifndef MQDLL_H_CREATEDBYCHENFAN_20130724_JSPTPD
#define MQDLL_H_CREATEDBYCHENFAN_20130724_JSPTPD

// mqdll.h
#define IMEXPORTS

#ifdef IMEXPORTS
  #define  DLLAPI __declspec(dllexport)
#else
  #define  DLLAPI __declspec(dllimport)
#endif

int DLLAPI max(int a,int b);

class DLLAPI DLLMain
{
public:
	int a;
public:
	DLLMain();
	~DLLMain();
};


#endif