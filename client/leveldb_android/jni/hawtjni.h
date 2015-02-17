/*******************************************************************************
* Copyright (C) 2011, FuseSource Corp.  All rights reserved.
*
*     http://fusesource.com
*
* The software in this package is published under the terms of the
* CDDL license a copy of which has been included with this distribution
* in the license.txt file.
*******************************************************************************/
/**
 * hawtjni.h
 *
 * This file contains the global macro declarations for a hawtjni based 
 * library.
 *
 */
#ifndef INC_HAWTJNI_H
#define INC_HAWTJNI_H
#define _MULTI_THREADED

#ifdef HAVE_CONFIG_H
  #include "config.h"
#endif

#include "jni.h"
#include <stdint.h>


#ifdef __cplusplus
extern "C" {
#endif

extern int IS_JNI_1_2;

#ifndef JNI64
#if __x86_64__
#define JNI64
#endif
#endif

/* 64 bit support */
#ifndef JNI64

/* int/long defines */
#define GetIntLongField GetIntField
#define SetIntLongField SetIntField
#define GetIntLongArrayElements GetIntArrayElements
#define ReleaseIntLongArrayElements ReleaseIntArrayElements
#define GetIntLongArrayRegion GetIntArrayRegion
#define SetIntLongArrayRegion SetIntArrayRegion
#define NewIntLongArray NewIntArray
#define CallStaticIntLongMethod CallStaticIntMethod
#define CallIntLongMethod CallIntMethod
#define CallStaticIntLongMethodV CallStaticIntMethodV
#define CallIntLongMethodV CallIntMethodV
#define jintLongArray jintArray
#define jintLong jint
#define I_J "I"
#define I_JArray "[I"

/* float/double defines */
#define GetFloatDoubleField GetFloatField
#define SetFloatDoubleField SetFloatField
#define GetFloatDoubleArrayElements GetFloatArrayElements
#define ReleaseFloatDoubleArrayElements ReleaseFloatArrayElements
#define GetFloatDoubleArrayRegion GetFloatArrayRegion
#define jfloatDoubleArray jfloatArray
#define jfloatDouble jfloat
#define F_D "F"
#define F_DArray "[F"

#else
	
/* int/long defines */
#define GetIntLongField GetLongField
#define SetIntLongField SetLongField
#define GetIntLongArrayElements GetLongArrayElements
#define ReleaseIntLongArrayElements ReleaseLongArrayElements
#define GetIntLongArrayRegion GetLongArrayRegion
#define SetIntLongArrayRegion SetLongArrayRegion
#define NewIntLongArray NewLongArray
#define CallStaticIntLongMethod CallStaticLongMethod
#define CallIntLongMethod CallLongMethod
#define CallStaticIntLongMethodV CallStaticLongMethodV
#define CallIntLongMethodV CallLongMethodV
#define jintLongArray jlongArray
#define jintLong jlong
#define I_J "J"
#define I_JArray "[J"

/* float/double defines */
#define GetFloatDoubleField GetDoubleField
#define SetFloatDoubleField SetDoubleField
#define GetFloatDoubleArrayElements GetDoubleArrayElements
#define ReleaseFloatDoubleArrayElements ReleaseDoubleArrayElements
#define GetFloatDoubleArrayRegion GetDoubleArrayRegion
#define jfloatDoubleArray jdoubleArray
#define jfloatDouble jdouble
#define F_D "D"
#define F_DArray "[D"

#endif

#ifdef __APPLE__
#define CALLING_CONVENTION
#define LOAD_FUNCTION(var, name) \
		static int initialized = 0; \
		static void *var = NULL; \
		if (!initialized) { \
			CFBundleRef bundle = CFBundleGetBundleWithIdentifier(CFSTR(name##_LIB)); \
			if (bundle) var = CFBundleGetFunctionPointerForName(bundle, CFSTR(#name)); \
			initialized = 1; \
		} 
#elif defined (_WIN32) || defined (_WIN32_WCE)
#define CALLING_CONVENTION CALLBACK
#define LOAD_FUNCTION(var, name) \
		static int initialized = 0; \
		static FARPROC var = NULL; \
		if (!initialized) { \
			HMODULE hm = LoadLibrary(name##_LIB); \
			if (hm) var = GetProcAddress(hm, #name); \
			initialized = 1; \
		}
#else
#define CALLING_CONVENTION
#define LOAD_FUNCTION(var, name) \
		static int initialized = 0; \
		static void *var = NULL; \
		if (!initialized) { \
			void* handle = dlopen(name##_LIB, RTLD_LAZY); \
			if (handle) var = dlsym(handle, #name); \
			initialized = 1; \
		}
#endif

#ifdef JNI_VERSION_1_2
extern JavaVM *JVM;
jint hawtjni_attach_thread(JNIEnv **env, const char *thread_name);
jint hawtjni_detach_thread();
#endif

void throwOutOfMemory(JNIEnv *env);

#define CHECK_NULL_VOID(ptr) \
	if ((ptr) == NULL) { \
		throwOutOfMemory(env); \
		return; \
	}

#define CHECK_NULL(ptr) \
	if ((ptr) == NULL) { \
		throwOutOfMemory(env); \
		return 0; \
	}

#ifndef JNI64
	void ** hawtjni_malloc_pointer_array(JNIEnv *env, jlongArray array);
	void hawtjni_free_pointer_array(JNIEnv *env, jlongArray array, void **elems, jint mode);
#else
  #ifdef __cplusplus
	#define hawtjni_malloc_pointer_array(env, array) ( (void **)(intptr_t)env->GetLongArrayElements(array, NULL) )
	#define hawtjni_free_pointer_array(env, array, elems, mode) ( env->ReleaseLongArrayElements(array, (jlong*)elems, mode) )
  #else
	#define hawtjni_malloc_pointer_array(env, source) ( (void **)(intptr_t)(*env)->GetLongArrayElements(env, source, NULL) )
	#define hawtjni_free_pointer_array(env, array, elems, mode) ( (*env)->ReleaseLongArrayElements(env, array, (jlong*)elems, mode) )
  #endif 
#endif /* JNI64 */

#ifdef __cplusplus
}
#endif 

#endif /* ifndef INC_HAWTJNI_H */
