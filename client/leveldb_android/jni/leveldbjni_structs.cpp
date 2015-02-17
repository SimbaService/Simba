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
#include "hawtjni.h"
#include "leveldbjni_structs.h"

typedef struct ComparatorJNI_FID_CACHE {
	int cached;
	jclass clazz;
	jfieldID target, compare_method, name;
} ComparatorJNI_FID_CACHE;

ComparatorJNI_FID_CACHE ComparatorJNIFc;

void cacheComparatorJNIFields(JNIEnv *env, jobject lpObject)
{
	if (ComparatorJNIFc.cached) return;
	ComparatorJNIFc.clazz = env->GetObjectClass(lpObject);
	ComparatorJNIFc.target = env->GetFieldID(ComparatorJNIFc.clazz, "target", "J");
	ComparatorJNIFc.compare_method = env->GetFieldID(ComparatorJNIFc.clazz, "compare_method", "J");
	ComparatorJNIFc.name = env->GetFieldID(ComparatorJNIFc.clazz, "name", "J");
	ComparatorJNIFc.cached = 1;
}

struct JNIComparator *getComparatorJNIFields(JNIEnv *env, jobject lpObject, struct JNIComparator *lpStruct)
{
	if (!ComparatorJNIFc.cached) cacheComparatorJNIFields(env, lpObject);
	lpStruct->target = (jobject)(intptr_t)env->GetLongField(lpObject, ComparatorJNIFc.target);
	lpStruct->compare_method = (jmethodID)(intptr_t)env->GetLongField(lpObject, ComparatorJNIFc.compare_method);
	lpStruct->name = (const char *)(intptr_t)env->GetLongField(lpObject, ComparatorJNIFc.name);
	return lpStruct;
}

void setComparatorJNIFields(JNIEnv *env, jobject lpObject, struct JNIComparator *lpStruct)
{
	if (!ComparatorJNIFc.cached) cacheComparatorJNIFields(env, lpObject);
	env->SetLongField(lpObject, ComparatorJNIFc.target, (jlong)(intptr_t)lpStruct->target);
	env->SetLongField(lpObject, ComparatorJNIFc.compare_method, (jlong)(intptr_t)lpStruct->compare_method);
	env->SetLongField(lpObject, ComparatorJNIFc.name, (jlong)(intptr_t)lpStruct->name);
}

typedef struct LoggerJNI_FID_CACHE {
	int cached;
	jclass clazz;
	jfieldID target, log_method;
} LoggerJNI_FID_CACHE;

LoggerJNI_FID_CACHE LoggerJNIFc;

void cacheLoggerJNIFields(JNIEnv *env, jobject lpObject)
{
	if (LoggerJNIFc.cached) return;
	LoggerJNIFc.clazz = env->GetObjectClass(lpObject);
	LoggerJNIFc.target = env->GetFieldID(LoggerJNIFc.clazz, "target", "J");
	LoggerJNIFc.log_method = env->GetFieldID(LoggerJNIFc.clazz, "log_method", "J");
	LoggerJNIFc.cached = 1;
}

struct JNILogger *getLoggerJNIFields(JNIEnv *env, jobject lpObject, struct JNILogger *lpStruct)
{
	if (!LoggerJNIFc.cached) cacheLoggerJNIFields(env, lpObject);
	lpStruct->target = (jobject)(intptr_t)env->GetLongField(lpObject, LoggerJNIFc.target);
	lpStruct->log_method = (jmethodID)(intptr_t)env->GetLongField(lpObject, LoggerJNIFc.log_method);
	return lpStruct;
}

void setLoggerJNIFields(JNIEnv *env, jobject lpObject, struct JNILogger *lpStruct)
{
	if (!LoggerJNIFc.cached) cacheLoggerJNIFields(env, lpObject);
	env->SetLongField(lpObject, LoggerJNIFc.target, (jlong)(intptr_t)lpStruct->target);
	env->SetLongField(lpObject, LoggerJNIFc.log_method, (jlong)(intptr_t)lpStruct->log_method);
}

typedef struct NativeOptions_FID_CACHE {
	int cached;
	jclass clazz;
	jfieldID create_if_missing, error_if_exists, paranoid_checks, write_buffer_size, block_size, max_open_files, block_restart_interval, comparator, info_log, env, block_cache, compression;
} NativeOptions_FID_CACHE;

NativeOptions_FID_CACHE NativeOptionsFc;

void cacheNativeOptionsFields(JNIEnv *env, jobject lpObject)
{
	if (NativeOptionsFc.cached) return;
	NativeOptionsFc.clazz = env->GetObjectClass(lpObject);
	NativeOptionsFc.create_if_missing = env->GetFieldID(NativeOptionsFc.clazz, "create_if_missing", "Z");
	NativeOptionsFc.error_if_exists = env->GetFieldID(NativeOptionsFc.clazz, "error_if_exists", "Z");
	NativeOptionsFc.paranoid_checks = env->GetFieldID(NativeOptionsFc.clazz, "paranoid_checks", "Z");
	NativeOptionsFc.write_buffer_size = env->GetFieldID(NativeOptionsFc.clazz, "write_buffer_size", "J");
	NativeOptionsFc.block_size = env->GetFieldID(NativeOptionsFc.clazz, "block_size", "J");
	NativeOptionsFc.max_open_files = env->GetFieldID(NativeOptionsFc.clazz, "max_open_files", "I");
	NativeOptionsFc.block_restart_interval = env->GetFieldID(NativeOptionsFc.clazz, "block_restart_interval", "I");
	NativeOptionsFc.comparator = env->GetFieldID(NativeOptionsFc.clazz, "comparator", "J");
	NativeOptionsFc.info_log = env->GetFieldID(NativeOptionsFc.clazz, "info_log", "J");
	NativeOptionsFc.env = env->GetFieldID(NativeOptionsFc.clazz, "env", "J");
	NativeOptionsFc.block_cache = env->GetFieldID(NativeOptionsFc.clazz, "block_cache", "J");
	NativeOptionsFc.compression = env->GetFieldID(NativeOptionsFc.clazz, "compression", "I");
	NativeOptionsFc.cached = 1;
}

struct leveldb::Options *getNativeOptionsFields(JNIEnv *env, jobject lpObject, struct leveldb::Options *lpStruct)
{
	if (!NativeOptionsFc.cached) cacheNativeOptionsFields(env, lpObject);
	lpStruct->create_if_missing = env->GetBooleanField(lpObject, NativeOptionsFc.create_if_missing);
	lpStruct->error_if_exists = env->GetBooleanField(lpObject, NativeOptionsFc.error_if_exists);
	lpStruct->paranoid_checks = env->GetBooleanField(lpObject, NativeOptionsFc.paranoid_checks);
	lpStruct->write_buffer_size = (size_t)env->GetLongField(lpObject, NativeOptionsFc.write_buffer_size);
	lpStruct->block_size = (size_t)env->GetLongField(lpObject, NativeOptionsFc.block_size);
	lpStruct->max_open_files = env->GetIntField(lpObject, NativeOptionsFc.max_open_files);
	lpStruct->block_restart_interval = env->GetIntField(lpObject, NativeOptionsFc.block_restart_interval);
	lpStruct->comparator = (const leveldb::Comparator*)(intptr_t)env->GetLongField(lpObject, NativeOptionsFc.comparator);
	lpStruct->info_log = (leveldb::Logger*)(intptr_t)env->GetLongField(lpObject, NativeOptionsFc.info_log);
	lpStruct->env = (leveldb::Env*)(intptr_t)env->GetLongField(lpObject, NativeOptionsFc.env);
	lpStruct->block_cache = (leveldb::Cache*)(intptr_t)env->GetLongField(lpObject, NativeOptionsFc.block_cache);
	lpStruct->compression = (leveldb::CompressionType)env->GetIntField(lpObject, NativeOptionsFc.compression);
	return lpStruct;
}

void setNativeOptionsFields(JNIEnv *env, jobject lpObject, struct leveldb::Options *lpStruct)
{
	if (!NativeOptionsFc.cached) cacheNativeOptionsFields(env, lpObject);
	env->SetBooleanField(lpObject, NativeOptionsFc.create_if_missing, (jboolean)lpStruct->create_if_missing);
	env->SetBooleanField(lpObject, NativeOptionsFc.error_if_exists, (jboolean)lpStruct->error_if_exists);
	env->SetBooleanField(lpObject, NativeOptionsFc.paranoid_checks, (jboolean)lpStruct->paranoid_checks);
	env->SetLongField(lpObject, NativeOptionsFc.write_buffer_size, (jlong)lpStruct->write_buffer_size);
	env->SetLongField(lpObject, NativeOptionsFc.block_size, (jlong)lpStruct->block_size);
	env->SetIntField(lpObject, NativeOptionsFc.max_open_files, (jint)lpStruct->max_open_files);
	env->SetIntField(lpObject, NativeOptionsFc.block_restart_interval, (jint)lpStruct->block_restart_interval);
	env->SetLongField(lpObject, NativeOptionsFc.comparator, (jlong)(intptr_t)lpStruct->comparator);
	env->SetLongField(lpObject, NativeOptionsFc.info_log, (jlong)(intptr_t)lpStruct->info_log);
	env->SetLongField(lpObject, NativeOptionsFc.env, (jlong)(intptr_t)lpStruct->env);
	env->SetLongField(lpObject, NativeOptionsFc.block_cache, (jlong)(intptr_t)lpStruct->block_cache);
	env->SetIntField(lpObject, NativeOptionsFc.compression, (jint)lpStruct->compression);
}

typedef struct RangeJNI_FID_CACHE {
	int cached;
	jclass clazz;
	jfieldID start, limit;
} RangeJNI_FID_CACHE;

RangeJNI_FID_CACHE RangeJNIFc;

void cacheRangeJNIFields(JNIEnv *env, jobject lpObject)
{
	if (RangeJNIFc.cached) return;
	RangeJNIFc.clazz = env->GetObjectClass(lpObject);
	RangeJNIFc.start = env->GetFieldID(RangeJNIFc.clazz, "start", "Lorg/fusesource/leveldbjni/internal/NativeSlice;");
	RangeJNIFc.limit = env->GetFieldID(RangeJNIFc.clazz, "limit", "Lorg/fusesource/leveldbjni/internal/NativeSlice;");
	RangeJNIFc.cached = 1;
}

struct leveldb::Range *getRangeJNIFields(JNIEnv *env, jobject lpObject, struct leveldb::Range *lpStruct)
{
	if (!RangeJNIFc.cached) cacheRangeJNIFields(env, lpObject);
	{
	jobject lpObject1 = env->GetObjectField(lpObject, RangeJNIFc.start);
	if (lpObject1 != NULL) getNativeSliceFields(env, lpObject1, &lpStruct->start);
	}
	{
	jobject lpObject1 = env->GetObjectField(lpObject, RangeJNIFc.limit);
	if (lpObject1 != NULL) getNativeSliceFields(env, lpObject1, &lpStruct->limit);
	}
	return lpStruct;
}

void setRangeJNIFields(JNIEnv *env, jobject lpObject, struct leveldb::Range *lpStruct)
{
	if (!RangeJNIFc.cached) cacheRangeJNIFields(env, lpObject);
	{
	jobject lpObject1 = env->GetObjectField(lpObject, RangeJNIFc.start);
	if (lpObject1 != NULL) setNativeSliceFields(env, lpObject1, &lpStruct->start);
	}
	{
	jobject lpObject1 = env->GetObjectField(lpObject, RangeJNIFc.limit);
	if (lpObject1 != NULL) setNativeSliceFields(env, lpObject1, &lpStruct->limit);
	}
}

typedef struct NativeReadOptions_FID_CACHE {
	int cached;
	jclass clazz;
	jfieldID verify_checksums, fill_cache, snapshot;
} NativeReadOptions_FID_CACHE;

NativeReadOptions_FID_CACHE NativeReadOptionsFc;

void cacheNativeReadOptionsFields(JNIEnv *env, jobject lpObject)
{
	if (NativeReadOptionsFc.cached) return;
	NativeReadOptionsFc.clazz = env->GetObjectClass(lpObject);
	NativeReadOptionsFc.verify_checksums = env->GetFieldID(NativeReadOptionsFc.clazz, "verify_checksums", "Z");
	NativeReadOptionsFc.fill_cache = env->GetFieldID(NativeReadOptionsFc.clazz, "fill_cache", "Z");
	NativeReadOptionsFc.snapshot = env->GetFieldID(NativeReadOptionsFc.clazz, "snapshot", "J");
	NativeReadOptionsFc.cached = 1;
}

struct leveldb::ReadOptions *getNativeReadOptionsFields(JNIEnv *env, jobject lpObject, struct leveldb::ReadOptions *lpStruct)
{
	if (!NativeReadOptionsFc.cached) cacheNativeReadOptionsFields(env, lpObject);
	lpStruct->verify_checksums = env->GetBooleanField(lpObject, NativeReadOptionsFc.verify_checksums);
	lpStruct->fill_cache = env->GetBooleanField(lpObject, NativeReadOptionsFc.fill_cache);
	lpStruct->snapshot = (const leveldb::Snapshot*)(intptr_t)env->GetLongField(lpObject, NativeReadOptionsFc.snapshot);
	return lpStruct;
}

void setNativeReadOptionsFields(JNIEnv *env, jobject lpObject, struct leveldb::ReadOptions *lpStruct)
{
	if (!NativeReadOptionsFc.cached) cacheNativeReadOptionsFields(env, lpObject);
	env->SetBooleanField(lpObject, NativeReadOptionsFc.verify_checksums, (jboolean)lpStruct->verify_checksums);
	env->SetBooleanField(lpObject, NativeReadOptionsFc.fill_cache, (jboolean)lpStruct->fill_cache);
	env->SetLongField(lpObject, NativeReadOptionsFc.snapshot, (jlong)(intptr_t)lpStruct->snapshot);
}

typedef struct NativeSlice_FID_CACHE {
	int cached;
	jclass clazz;
	jfieldID data_, size_;
} NativeSlice_FID_CACHE;

NativeSlice_FID_CACHE NativeSliceFc;

void cacheNativeSliceFields(JNIEnv *env, jobject lpObject)
{
	if (NativeSliceFc.cached) return;
	NativeSliceFc.clazz = env->GetObjectClass(lpObject);
	NativeSliceFc.data_ = env->GetFieldID(NativeSliceFc.clazz, "data_", "J");
	NativeSliceFc.size_ = env->GetFieldID(NativeSliceFc.clazz, "size_", "J");
	NativeSliceFc.cached = 1;
}

struct leveldb::Slice *getNativeSliceFields(JNIEnv *env, jobject lpObject, struct leveldb::Slice *lpStruct)
{
	if (!NativeSliceFc.cached) cacheNativeSliceFields(env, lpObject);
	lpStruct->data_ = (const char*)(intptr_t)env->GetLongField(lpObject, NativeSliceFc.data_);
	lpStruct->size_ = (size_t)env->GetLongField(lpObject, NativeSliceFc.size_);
	return lpStruct;
}

void setNativeSliceFields(JNIEnv *env, jobject lpObject, struct leveldb::Slice *lpStruct)
{
	if (!NativeSliceFc.cached) cacheNativeSliceFields(env, lpObject);
	env->SetLongField(lpObject, NativeSliceFc.data_, (jlong)(intptr_t)lpStruct->data_);
	env->SetLongField(lpObject, NativeSliceFc.size_, (jlong)lpStruct->size_);
}

typedef struct NativeWriteOptions_FID_CACHE {
	int cached;
	jclass clazz;
	jfieldID sync;
} NativeWriteOptions_FID_CACHE;

NativeWriteOptions_FID_CACHE NativeWriteOptionsFc;

void cacheNativeWriteOptionsFields(JNIEnv *env, jobject lpObject)
{
	if (NativeWriteOptionsFc.cached) return;
	NativeWriteOptionsFc.clazz = env->GetObjectClass(lpObject);
	NativeWriteOptionsFc.sync = env->GetFieldID(NativeWriteOptionsFc.clazz, "sync", "Z");
	NativeWriteOptionsFc.cached = 1;
}

struct leveldb::WriteOptions *getNativeWriteOptionsFields(JNIEnv *env, jobject lpObject, struct leveldb::WriteOptions *lpStruct)
{
	if (!NativeWriteOptionsFc.cached) cacheNativeWriteOptionsFields(env, lpObject);
	lpStruct->sync = env->GetBooleanField(lpObject, NativeWriteOptionsFc.sync);
	return lpStruct;
}

void setNativeWriteOptionsFields(JNIEnv *env, jobject lpObject, struct leveldb::WriteOptions *lpStruct)
{
	if (!NativeWriteOptionsFc.cached) cacheNativeWriteOptionsFields(env, lpObject);
	env->SetBooleanField(lpObject, NativeWriteOptionsFc.sync, (jboolean)lpStruct->sync);
}

