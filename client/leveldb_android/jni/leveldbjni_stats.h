/*******************************************************************************
* Copyright (C) 2011, FuseSource Corp.  All rights reserved.
*
*     http://fusesource.com
*
* The software in this package is published under the terms of the
* CDDL license a copy of which has been included with this distribution
* in the license.txt file.
*******************************************************************************/
#ifdef NATIVE_STATS
extern int NativeBufferJNI_nativeFunctionCount;
extern int NativeBufferJNI_nativeFunctionCallCount[];
extern char* NativeBufferJNI_nativeFunctionNames[];
#define NativeBufferJNI_NATIVE_ENTER(env, that, func) NativeBufferJNI_nativeFunctionCallCount[func]++;
#define NativeBufferJNI_NATIVE_EXIT(env, that, func) 
#else
#ifndef NativeBufferJNI_NATIVE_ENTER
#define NativeBufferJNI_NATIVE_ENTER(env, that, func) 
#endif
#ifndef NativeBufferJNI_NATIVE_EXIT
#define NativeBufferJNI_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	NativeBufferJNI_buffer_1copy__JJ_3BJJ_FUNC,
	NativeBufferJNI_buffer_1copy___3BJJJJ_FUNC,
	NativeBufferJNI_free_FUNC,
	NativeBufferJNI_malloc_FUNC,
} NativeBufferJNI_FUNCS;
#ifdef NATIVE_STATS
extern int CacheJNI_nativeFunctionCount;
extern int CacheJNI_nativeFunctionCallCount[];
extern char* CacheJNI_nativeFunctionNames[];
#define CacheJNI_NATIVE_ENTER(env, that, func) CacheJNI_nativeFunctionCallCount[func]++;
#define CacheJNI_NATIVE_EXIT(env, that, func) 
#else
#ifndef CacheJNI_NATIVE_ENTER
#define CacheJNI_NATIVE_ENTER(env, that, func) 
#endif
#ifndef CacheJNI_NATIVE_EXIT
#define CacheJNI_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	CacheJNI_NewLRUCache_FUNC,
	CacheJNI_delete_FUNC,
} CacheJNI_FUNCS;
#ifdef NATIVE_STATS
extern int ComparatorJNI_nativeFunctionCount;
extern int ComparatorJNI_nativeFunctionCallCount[];
extern char* ComparatorJNI_nativeFunctionNames[];
#define ComparatorJNI_NATIVE_ENTER(env, that, func) ComparatorJNI_nativeFunctionCallCount[func]++;
#define ComparatorJNI_NATIVE_EXIT(env, that, func) 
#else
#ifndef ComparatorJNI_NATIVE_ENTER
#define ComparatorJNI_NATIVE_ENTER(env, that, func) 
#endif
#ifndef ComparatorJNI_NATIVE_EXIT
#define ComparatorJNI_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	ComparatorJNI_create_FUNC,
	ComparatorJNI_delete_FUNC,
	ComparatorJNI_init_FUNC,
	ComparatorJNI_memmove__JLorg_fusesource_leveldbjni_internal_NativeComparator_00024ComparatorJNI_2J_FUNC,
	ComparatorJNI_memmove__Lorg_fusesource_leveldbjni_internal_NativeComparator_00024ComparatorJNI_2JJ_FUNC,
} ComparatorJNI_FUNCS;
#ifdef NATIVE_STATS
extern int DBJNI_nativeFunctionCount;
extern int DBJNI_nativeFunctionCallCount[];
extern char* DBJNI_nativeFunctionNames[];
#define DBJNI_NATIVE_ENTER(env, that, func) DBJNI_nativeFunctionCallCount[func]++;
#define DBJNI_NATIVE_EXIT(env, that, func) 
#else
#ifndef DBJNI_NATIVE_ENTER
#define DBJNI_NATIVE_ENTER(env, that, func) 
#endif
#ifndef DBJNI_NATIVE_EXIT
#define DBJNI_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	DBJNI_CompactRange_FUNC,
	DBJNI_Delete_FUNC,
	DBJNI_DeleteGlobalRef_FUNC,
	DBJNI_DestroyDB_FUNC,
	DBJNI_Get_FUNC,
	DBJNI_GetApproximateSizes_FUNC,
	DBJNI_GetMethodID_FUNC,
	DBJNI_GetProperty_FUNC,
	DBJNI_GetSnapshot_FUNC,
	DBJNI_NewGlobalRef_FUNC,
	DBJNI_NewIterator_FUNC,
	DBJNI_Open_FUNC,
	DBJNI_Put_FUNC,
	DBJNI_ReleaseSnapshot_FUNC,
	DBJNI_RepairDB_FUNC,
	DBJNI_ResumeCompactions_FUNC,
	DBJNI_SuspendCompactions_FUNC,
	DBJNI_Write_FUNC,
	DBJNI_delete_FUNC,
} DBJNI_FUNCS;
#ifdef NATIVE_STATS
extern int IteratorJNI_nativeFunctionCount;
extern int IteratorJNI_nativeFunctionCallCount[];
extern char* IteratorJNI_nativeFunctionNames[];
#define IteratorJNI_NATIVE_ENTER(env, that, func) IteratorJNI_nativeFunctionCallCount[func]++;
#define IteratorJNI_NATIVE_EXIT(env, that, func) 
#else
#ifndef IteratorJNI_NATIVE_ENTER
#define IteratorJNI_NATIVE_ENTER(env, that, func) 
#endif
#ifndef IteratorJNI_NATIVE_EXIT
#define IteratorJNI_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	IteratorJNI_Next_FUNC,
	IteratorJNI_Prev_FUNC,
	IteratorJNI_Seek_FUNC,
	IteratorJNI_SeekToFirst_FUNC,
	IteratorJNI_SeekToLast_FUNC,
	IteratorJNI_Valid_FUNC,
	IteratorJNI_delete_FUNC,
	IteratorJNI_key_FUNC,
	IteratorJNI_status_FUNC,
	IteratorJNI_value_FUNC,
} IteratorJNI_FUNCS;
#ifdef NATIVE_STATS
extern int LoggerJNI_nativeFunctionCount;
extern int LoggerJNI_nativeFunctionCallCount[];
extern char* LoggerJNI_nativeFunctionNames[];
#define LoggerJNI_NATIVE_ENTER(env, that, func) LoggerJNI_nativeFunctionCallCount[func]++;
#define LoggerJNI_NATIVE_EXIT(env, that, func) 
#else
#ifndef LoggerJNI_NATIVE_ENTER
#define LoggerJNI_NATIVE_ENTER(env, that, func) 
#endif
#ifndef LoggerJNI_NATIVE_EXIT
#define LoggerJNI_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	LoggerJNI_create_FUNC,
	LoggerJNI_delete_FUNC,
	LoggerJNI_init_FUNC,
	LoggerJNI_memmove_FUNC,
} LoggerJNI_FUNCS;
#ifdef NATIVE_STATS
extern int NativeOptions_nativeFunctionCount;
extern int NativeOptions_nativeFunctionCallCount[];
extern char* NativeOptions_nativeFunctionNames[];
#define NativeOptions_NATIVE_ENTER(env, that, func) NativeOptions_nativeFunctionCallCount[func]++;
#define NativeOptions_NATIVE_EXIT(env, that, func) 
#else
#ifndef NativeOptions_NATIVE_ENTER
#define NativeOptions_NATIVE_ENTER(env, that, func) 
#endif
#ifndef NativeOptions_NATIVE_EXIT
#define NativeOptions_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	NativeOptions_init_FUNC,
} NativeOptions_FUNCS;
#ifdef NATIVE_STATS
extern int RangeJNI_nativeFunctionCount;
extern int RangeJNI_nativeFunctionCallCount[];
extern char* RangeJNI_nativeFunctionNames[];
#define RangeJNI_NATIVE_ENTER(env, that, func) RangeJNI_nativeFunctionCallCount[func]++;
#define RangeJNI_NATIVE_EXIT(env, that, func) 
#else
#ifndef RangeJNI_NATIVE_ENTER
#define RangeJNI_NATIVE_ENTER(env, that, func) 
#endif
#ifndef RangeJNI_NATIVE_EXIT
#define RangeJNI_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	RangeJNI_init_FUNC,
	RangeJNI_memmove__JLorg_fusesource_leveldbjni_internal_NativeRange_00024RangeJNI_2J_FUNC,
	RangeJNI_memmove__Lorg_fusesource_leveldbjni_internal_NativeRange_00024RangeJNI_2JJ_FUNC,
} RangeJNI_FUNCS;
#ifdef NATIVE_STATS
extern int SliceJNI_nativeFunctionCount;
extern int SliceJNI_nativeFunctionCallCount[];
extern char* SliceJNI_nativeFunctionNames[];
#define SliceJNI_NATIVE_ENTER(env, that, func) SliceJNI_nativeFunctionCallCount[func]++;
#define SliceJNI_NATIVE_EXIT(env, that, func) 
#else
#ifndef SliceJNI_NATIVE_ENTER
#define SliceJNI_NATIVE_ENTER(env, that, func) 
#endif
#ifndef SliceJNI_NATIVE_EXIT
#define SliceJNI_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	SliceJNI_delete_FUNC,
	SliceJNI_init_FUNC,
	SliceJNI_memmove__JLorg_fusesource_leveldbjni_internal_NativeSlice_2J_FUNC,
	SliceJNI_memmove__Lorg_fusesource_leveldbjni_internal_NativeSlice_2JJ_FUNC,
} SliceJNI_FUNCS;
#ifdef NATIVE_STATS
extern int StatusJNI_nativeFunctionCount;
extern int StatusJNI_nativeFunctionCallCount[];
extern char* StatusJNI_nativeFunctionNames[];
#define StatusJNI_NATIVE_ENTER(env, that, func) StatusJNI_nativeFunctionCallCount[func]++;
#define StatusJNI_NATIVE_EXIT(env, that, func) 
#else
#ifndef StatusJNI_NATIVE_ENTER
#define StatusJNI_NATIVE_ENTER(env, that, func) 
#endif
#ifndef StatusJNI_NATIVE_EXIT
#define StatusJNI_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	StatusJNI_IsNotFound_FUNC,
	StatusJNI_ToString_FUNC,
	StatusJNI_delete_FUNC,
	StatusJNI_ok_FUNC,
} StatusJNI_FUNCS;
#ifdef NATIVE_STATS
extern int StdStringJNI_nativeFunctionCount;
extern int StdStringJNI_nativeFunctionCallCount[];
extern char* StdStringJNI_nativeFunctionNames[];
#define StdStringJNI_NATIVE_ENTER(env, that, func) StdStringJNI_nativeFunctionCallCount[func]++;
#define StdStringJNI_NATIVE_EXIT(env, that, func) 
#else
#ifndef StdStringJNI_NATIVE_ENTER
#define StdStringJNI_NATIVE_ENTER(env, that, func) 
#endif
#ifndef StdStringJNI_NATIVE_EXIT
#define StdStringJNI_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	StdStringJNI_c_1str_1ptr_FUNC,
	StdStringJNI_create___FUNC,
	StdStringJNI_create__Ljava_lang_String_2_FUNC,
	StdStringJNI_delete_FUNC,
	StdStringJNI_length_FUNC,
} StdStringJNI_FUNCS;
#ifdef NATIVE_STATS
extern int WriteBatchJNI_nativeFunctionCount;
extern int WriteBatchJNI_nativeFunctionCallCount[];
extern char* WriteBatchJNI_nativeFunctionNames[];
#define WriteBatchJNI_NATIVE_ENTER(env, that, func) WriteBatchJNI_nativeFunctionCallCount[func]++;
#define WriteBatchJNI_NATIVE_EXIT(env, that, func) 
#else
#ifndef WriteBatchJNI_NATIVE_ENTER
#define WriteBatchJNI_NATIVE_ENTER(env, that, func) 
#endif
#ifndef WriteBatchJNI_NATIVE_EXIT
#define WriteBatchJNI_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	WriteBatchJNI_Clear_FUNC,
	WriteBatchJNI_Delete_FUNC,
	WriteBatchJNI_Put_FUNC,
	WriteBatchJNI_create_FUNC,
	WriteBatchJNI_delete_FUNC,
} WriteBatchJNI_FUNCS;
#ifdef NATIVE_STATS
extern int EnvJNI_nativeFunctionCount;
extern int EnvJNI_nativeFunctionCallCount[];
extern char* EnvJNI_nativeFunctionNames[];
#define EnvJNI_NATIVE_ENTER(env, that, func) EnvJNI_nativeFunctionCallCount[func]++;
#define EnvJNI_NATIVE_EXIT(env, that, func) 
#else
#ifndef EnvJNI_NATIVE_ENTER
#define EnvJNI_NATIVE_ENTER(env, that, func) 
#endif
#ifndef EnvJNI_NATIVE_EXIT
#define EnvJNI_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	EnvJNI_Default_FUNC,
	EnvJNI_Schedule_FUNC,
} EnvJNI_FUNCS;
#ifdef NATIVE_STATS
extern int UtilJNI_nativeFunctionCount;
extern int UtilJNI_nativeFunctionCallCount[];
extern char* UtilJNI_nativeFunctionNames[];
#define UtilJNI_NATIVE_ENTER(env, that, func) UtilJNI_nativeFunctionCallCount[func]++;
#define UtilJNI_NATIVE_EXIT(env, that, func) 
#else
#ifndef UtilJNI_NATIVE_ENTER
#define UtilJNI_NATIVE_ENTER(env, that, func) 
#endif
#ifndef UtilJNI_NATIVE_EXIT
#define UtilJNI_NATIVE_EXIT(env, that, func) 
#endif
#endif

typedef enum {
	UtilJNI_CreateHardLinkW_FUNC,
	UtilJNI_errno_FUNC,
	UtilJNI_init_FUNC,
	UtilJNI_link_FUNC,
	UtilJNI_strerror_FUNC,
	UtilJNI_strlen_FUNC,
} UtilJNI_FUNCS;
