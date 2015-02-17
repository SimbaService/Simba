/*******************************************************************************
* Copyright (C) 2011, FuseSource Corp.  All rights reserved.
*
*     http://fusesource.com
*
* The software in this package is published under the terms of the
* CDDL license a copy of which has been included with this distribution
* in the license.txt file.
*******************************************************************************/
#include "hawtjni.h"
#include "leveldbjni_stats.h"

#ifdef NATIVE_STATS

int NativeBufferJNI_nativeFunctionCount = 4;
int NativeBufferJNI_nativeFunctionCallCount[4];
char * NativeBufferJNI_nativeFunctionNames[] = {
	"buffer_1copy__JJ_3BJJ",
	"buffer_1copy___3BJJJJ",
	"free",
	"malloc",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(NativeBufferJNI_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return NativeBufferJNI_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(NativeBufferJNI_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(NativeBufferJNI_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(NativeBufferJNI_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return NativeBufferJNI_nativeFunctionCallCount[index];
}

#endif
#ifdef NATIVE_STATS

int CacheJNI_nativeFunctionCount = 2;
int CacheJNI_nativeFunctionCallCount[2];
char * CacheJNI_nativeFunctionNames[] = {
	"NewLRUCache",
	"delete",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(CacheJNI_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return CacheJNI_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(CacheJNI_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(CacheJNI_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(CacheJNI_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return CacheJNI_nativeFunctionCallCount[index];
}

#endif
#ifdef NATIVE_STATS

int ComparatorJNI_nativeFunctionCount = 5;
int ComparatorJNI_nativeFunctionCallCount[5];
char * ComparatorJNI_nativeFunctionNames[] = {
	"create",
	"delete",
	"init",
	"memmove__JLorg_fusesource_leveldbjni_internal_NativeComparator_00024ComparatorJNI_2J",
	"memmove__Lorg_fusesource_leveldbjni_internal_NativeComparator_00024ComparatorJNI_2JJ",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(ComparatorJNI_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return ComparatorJNI_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(ComparatorJNI_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(ComparatorJNI_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(ComparatorJNI_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return ComparatorJNI_nativeFunctionCallCount[index];
}

#endif
#ifdef NATIVE_STATS

int DBJNI_nativeFunctionCount = 19;
int DBJNI_nativeFunctionCallCount[19];
char * DBJNI_nativeFunctionNames[] = {
	"CompactRange",
	"Delete",
	"DeleteGlobalRef",
	"DestroyDB",
	"Get",
	"GetApproximateSizes",
	"GetMethodID",
	"GetProperty",
	"GetSnapshot",
	"NewGlobalRef",
	"NewIterator",
	"Open",
	"Put",
	"ReleaseSnapshot",
	"RepairDB",
	"ResumeCompactions",
	"SuspendCompactions",
	"Write",
	"delete",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(DBJNI_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return DBJNI_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(DBJNI_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(DBJNI_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(DBJNI_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return DBJNI_nativeFunctionCallCount[index];
}

#endif
#ifdef NATIVE_STATS

int IteratorJNI_nativeFunctionCount = 10;
int IteratorJNI_nativeFunctionCallCount[10];
char * IteratorJNI_nativeFunctionNames[] = {
	"Next",
	"Prev",
	"Seek",
	"SeekToFirst",
	"SeekToLast",
	"Valid",
	"delete",
	"key",
	"status",
	"value",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(IteratorJNI_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return IteratorJNI_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(IteratorJNI_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(IteratorJNI_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(IteratorJNI_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return IteratorJNI_nativeFunctionCallCount[index];
}

#endif
#ifdef NATIVE_STATS

int LoggerJNI_nativeFunctionCount = 4;
int LoggerJNI_nativeFunctionCallCount[4];
char * LoggerJNI_nativeFunctionNames[] = {
	"create",
	"delete",
	"init",
	"memmove",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(LoggerJNI_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return LoggerJNI_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(LoggerJNI_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(LoggerJNI_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(LoggerJNI_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return LoggerJNI_nativeFunctionCallCount[index];
}

#endif
#ifdef NATIVE_STATS

int NativeOptions_nativeFunctionCount = 1;
int NativeOptions_nativeFunctionCallCount[1];
char * NativeOptions_nativeFunctionNames[] = {
	"init",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(NativeOptions_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return NativeOptions_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(NativeOptions_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(NativeOptions_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(NativeOptions_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return NativeOptions_nativeFunctionCallCount[index];
}

#endif
#ifdef NATIVE_STATS

int RangeJNI_nativeFunctionCount = 3;
int RangeJNI_nativeFunctionCallCount[3];
char * RangeJNI_nativeFunctionNames[] = {
	"init",
	"memmove__JLorg_fusesource_leveldbjni_internal_NativeRange_00024RangeJNI_2J",
	"memmove__Lorg_fusesource_leveldbjni_internal_NativeRange_00024RangeJNI_2JJ",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(RangeJNI_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return RangeJNI_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(RangeJNI_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(RangeJNI_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(RangeJNI_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return RangeJNI_nativeFunctionCallCount[index];
}

#endif
#ifdef NATIVE_STATS

int SliceJNI_nativeFunctionCount = 4;
int SliceJNI_nativeFunctionCallCount[4];
char * SliceJNI_nativeFunctionNames[] = {
	"delete",
	"init",
	"memmove__JLorg_fusesource_leveldbjni_internal_NativeSlice_2J",
	"memmove__Lorg_fusesource_leveldbjni_internal_NativeSlice_2JJ",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(SliceJNI_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return SliceJNI_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(SliceJNI_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(SliceJNI_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(SliceJNI_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return SliceJNI_nativeFunctionCallCount[index];
}

#endif
#ifdef NATIVE_STATS

int StatusJNI_nativeFunctionCount = 4;
int StatusJNI_nativeFunctionCallCount[4];
char * StatusJNI_nativeFunctionNames[] = {
	"IsNotFound",
	"ToString",
	"delete",
	"ok",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(StatusJNI_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return StatusJNI_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(StatusJNI_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(StatusJNI_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(StatusJNI_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return StatusJNI_nativeFunctionCallCount[index];
}

#endif
#ifdef NATIVE_STATS

int StdStringJNI_nativeFunctionCount = 5;
int StdStringJNI_nativeFunctionCallCount[5];
char * StdStringJNI_nativeFunctionNames[] = {
	"c_1str_1ptr",
	"create__",
	"create__Ljava_lang_String_2",
	"delete",
	"length",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(StdStringJNI_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return StdStringJNI_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(StdStringJNI_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(StdStringJNI_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(StdStringJNI_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return StdStringJNI_nativeFunctionCallCount[index];
}

#endif
#ifdef NATIVE_STATS

int WriteBatchJNI_nativeFunctionCount = 5;
int WriteBatchJNI_nativeFunctionCallCount[5];
char * WriteBatchJNI_nativeFunctionNames[] = {
	"Clear",
	"Delete",
	"Put",
	"create",
	"delete",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(WriteBatchJNI_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return WriteBatchJNI_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(WriteBatchJNI_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(WriteBatchJNI_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(WriteBatchJNI_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return WriteBatchJNI_nativeFunctionCallCount[index];
}

#endif
#ifdef NATIVE_STATS

int EnvJNI_nativeFunctionCount = 2;
int EnvJNI_nativeFunctionCallCount[2];
char * EnvJNI_nativeFunctionNames[] = {
	"Default",
	"Schedule",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(EnvJNI_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return EnvJNI_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(EnvJNI_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(EnvJNI_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(EnvJNI_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return EnvJNI_nativeFunctionCallCount[index];
}

#endif
#ifdef NATIVE_STATS

int UtilJNI_nativeFunctionCount = 6;
int UtilJNI_nativeFunctionCallCount[6];
char * UtilJNI_nativeFunctionNames[] = {
	"CreateHardLinkW",
	"errno",
	"init",
	"link",
	"strerror",
	"strlen",
};

#define STATS_NATIVE(func) Java_org_fusesource_hawtjni_runtime_NativeStats_##func

JNIEXPORT jint JNICALL STATS_NATIVE(UtilJNI_1GetFunctionCount)
	(JNIEnv *env, jclass that)
{
	return UtilJNI_nativeFunctionCount;
}

JNIEXPORT jstring JNICALL STATS_NATIVE(UtilJNI_1GetFunctionName)
	(JNIEnv *env, jclass that, jint index)
{
	return env->NewStringUTF(UtilJNI_nativeFunctionNames[index]);
}

JNIEXPORT jint JNICALL STATS_NATIVE(UtilJNI_1GetFunctionCallCount)
	(JNIEnv *env, jclass that, jint index)
{
	return UtilJNI_nativeFunctionCallCount[index];
}

#endif
