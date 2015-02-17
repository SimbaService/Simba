/*******************************************************************************
* Copyright (C) 2011, FuseSource Corp.  All rights reserved.
*
*     http://fusesource.com
*
* The software in this package is published under the terms of the
* CDDL license a copy of which has been included with this distribution
* in the license.txt file.
*******************************************************************************/
#include "leveldbjni.h"

void cacheComparatorJNIFields(JNIEnv *env, jobject lpObject);
struct JNIComparator *getComparatorJNIFields(JNIEnv *env, jobject lpObject, struct JNIComparator *lpStruct);
void setComparatorJNIFields(JNIEnv *env, jobject lpObject, struct JNIComparator *lpStruct);

void cacheLoggerJNIFields(JNIEnv *env, jobject lpObject);
struct JNILogger *getLoggerJNIFields(JNIEnv *env, jobject lpObject, struct JNILogger *lpStruct);
void setLoggerJNIFields(JNIEnv *env, jobject lpObject, struct JNILogger *lpStruct);

void cacheNativeOptionsFields(JNIEnv *env, jobject lpObject);
struct leveldb::Options *getNativeOptionsFields(JNIEnv *env, jobject lpObject, struct leveldb::Options *lpStruct);
void setNativeOptionsFields(JNIEnv *env, jobject lpObject, struct leveldb::Options *lpStruct);

void cacheRangeJNIFields(JNIEnv *env, jobject lpObject);
struct leveldb::Range *getRangeJNIFields(JNIEnv *env, jobject lpObject, struct leveldb::Range *lpStruct);
void setRangeJNIFields(JNIEnv *env, jobject lpObject, struct leveldb::Range *lpStruct);

void cacheNativeReadOptionsFields(JNIEnv *env, jobject lpObject);
struct leveldb::ReadOptions *getNativeReadOptionsFields(JNIEnv *env, jobject lpObject, struct leveldb::ReadOptions *lpStruct);
void setNativeReadOptionsFields(JNIEnv *env, jobject lpObject, struct leveldb::ReadOptions *lpStruct);

void cacheNativeSliceFields(JNIEnv *env, jobject lpObject);
struct leveldb::Slice *getNativeSliceFields(JNIEnv *env, jobject lpObject, struct leveldb::Slice *lpStruct);
void setNativeSliceFields(JNIEnv *env, jobject lpObject, struct leveldb::Slice *lpStruct);

void cacheNativeWriteOptionsFields(JNIEnv *env, jobject lpObject);
struct leveldb::WriteOptions *getNativeWriteOptionsFields(JNIEnv *env, jobject lpObject, struct leveldb::WriteOptions *lpStruct);
void setNativeWriteOptionsFields(JNIEnv *env, jobject lpObject, struct leveldb::WriteOptions *lpStruct);

