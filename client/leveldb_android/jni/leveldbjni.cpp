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
#include "leveldbjni_stats.h"

#define NativeBufferJNI_NATIVE(func) Java_org_fusesource_leveldbjni_internal_NativeBuffer_00024NativeBufferJNI_##func

extern "C" JNIEXPORT void JNICALL NativeBufferJNI_NATIVE(buffer_1copy__JJ_3BJJ)(JNIEnv *env, jclass that, jlong arg0, jlong arg1, jbyteArray arg2, jlong arg3, jlong arg4);
JNIEXPORT void JNICALL NativeBufferJNI_NATIVE(buffer_1copy__JJ_3BJJ)
	(JNIEnv *env, jclass that, jlong arg0, jlong arg1, jbyteArray arg2, jlong arg3, jlong arg4)
{
	jbyte *lparg2=NULL;
	NativeBufferJNI_NATIVE_ENTER(env, that, NativeBufferJNI_buffer_1copy__JJ_3BJJ_FUNC);
#ifdef JNI_VERSION_1_2
	if (IS_JNI_1_2) {
		if (arg2) if ((lparg2 = (jbyte*)env->GetPrimitiveArrayCritical(arg2, NULL)) == NULL) goto fail;
	} else
#endif
	{
		if (arg2) if ((lparg2 = env->GetByteArrayElements(arg2, NULL)) == NULL) goto fail;
	}
	buffer_copy((const void *)(intptr_t)arg0, (size_t)arg1, (void *)lparg2, (size_t)arg3, (size_t)arg4);
fail:
#ifdef JNI_VERSION_1_2
	if (IS_JNI_1_2) {
		if (arg2 && lparg2) env->ReleasePrimitiveArrayCritical(arg2, lparg2, 0);
	} else
#endif
	{
		if (arg2 && lparg2) env->ReleaseByteArrayElements(arg2, lparg2, 0);
	}
	NativeBufferJNI_NATIVE_EXIT(env, that, NativeBufferJNI_buffer_1copy__JJ_3BJJ_FUNC);
}

extern "C" JNIEXPORT void JNICALL NativeBufferJNI_NATIVE(buffer_1copy___3BJJJJ)(JNIEnv *env, jclass that, jbyteArray arg0, jlong arg1, jlong arg2, jlong arg3, jlong arg4);
JNIEXPORT void JNICALL NativeBufferJNI_NATIVE(buffer_1copy___3BJJJJ)
	(JNIEnv *env, jclass that, jbyteArray arg0, jlong arg1, jlong arg2, jlong arg3, jlong arg4)
{
	jbyte *lparg0=NULL;
	NativeBufferJNI_NATIVE_ENTER(env, that, NativeBufferJNI_buffer_1copy___3BJJJJ_FUNC);
#ifdef JNI_VERSION_1_2
	if (IS_JNI_1_2) {
		if (arg0) if ((lparg0 = (jbyte*)env->GetPrimitiveArrayCritical(arg0, NULL)) == NULL) goto fail;
	} else
#endif
	{
		if (arg0) if ((lparg0 = env->GetByteArrayElements(arg0, NULL)) == NULL) goto fail;
	}
	buffer_copy((const void *)lparg0, (size_t)arg1, (void *)(intptr_t)arg2, (size_t)arg3, (size_t)arg4);
fail:
#ifdef JNI_VERSION_1_2
	if (IS_JNI_1_2) {
		if (arg0 && lparg0) env->ReleasePrimitiveArrayCritical(arg0, lparg0, JNI_ABORT);
	} else
#endif
	{
		if (arg0 && lparg0) env->ReleaseByteArrayElements(arg0, lparg0, JNI_ABORT);
	}
	NativeBufferJNI_NATIVE_EXIT(env, that, NativeBufferJNI_buffer_1copy___3BJJJJ_FUNC);
}

extern "C" JNIEXPORT void JNICALL NativeBufferJNI_NATIVE(free)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL NativeBufferJNI_NATIVE(free)
	(JNIEnv *env, jclass that, jlong arg0)
{
	NativeBufferJNI_NATIVE_ENTER(env, that, NativeBufferJNI_free_FUNC);
	free((void *)(intptr_t)arg0);
	NativeBufferJNI_NATIVE_EXIT(env, that, NativeBufferJNI_free_FUNC);
}

extern "C" JNIEXPORT jlong JNICALL NativeBufferJNI_NATIVE(malloc)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT jlong JNICALL NativeBufferJNI_NATIVE(malloc)
	(JNIEnv *env, jclass that, jlong arg0)
{
	jlong rc = 0;
	NativeBufferJNI_NATIVE_ENTER(env, that, NativeBufferJNI_malloc_FUNC);
	rc = (intptr_t)(void *)malloc((size_t)arg0);
	NativeBufferJNI_NATIVE_EXIT(env, that, NativeBufferJNI_malloc_FUNC);
	return rc;
}

#define CacheJNI_NATIVE(func) Java_org_fusesource_leveldbjni_internal_NativeCache_00024CacheJNI_##func

extern "C" JNIEXPORT jlong JNICALL CacheJNI_NATIVE(NewLRUCache)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT jlong JNICALL CacheJNI_NATIVE(NewLRUCache)
	(JNIEnv *env, jclass that, jlong arg0)
{
	jlong rc = 0;
	CacheJNI_NATIVE_ENTER(env, that, CacheJNI_NewLRUCache_FUNC);
	rc = (intptr_t)(leveldb::Cache *)leveldb::NewLRUCache((size_t)arg0);
	CacheJNI_NATIVE_EXIT(env, that, CacheJNI_NewLRUCache_FUNC);
	return rc;
}

extern "C" JNIEXPORT void JNICALL CacheJNI_NATIVE(delete)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL CacheJNI_NATIVE(delete)
	(JNIEnv *env, jclass that, jlong arg0)
{
	CacheJNI_NATIVE_ENTER(env, that, CacheJNI_delete_FUNC);
	delete (leveldb::Cache *)(intptr_t)arg0;
	CacheJNI_NATIVE_EXIT(env, that, CacheJNI_delete_FUNC);
}

#define ComparatorJNI_NATIVE(func) Java_org_fusesource_leveldbjni_internal_NativeComparator_00024ComparatorJNI_##func

extern "C" JNIEXPORT jlong JNICALL ComparatorJNI_NATIVE(create)(JNIEnv *env, jclass that);
JNIEXPORT jlong JNICALL ComparatorJNI_NATIVE(create)
	(JNIEnv *env, jclass that)
{
	jlong rc = 0;
	ComparatorJNI_NATIVE_ENTER(env, that, ComparatorJNI_create_FUNC);
	rc = (intptr_t)(JNIComparator *)new JNIComparator();
	ComparatorJNI_NATIVE_EXIT(env, that, ComparatorJNI_create_FUNC);
	return rc;
}

extern "C" JNIEXPORT void JNICALL ComparatorJNI_NATIVE(delete)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL ComparatorJNI_NATIVE(delete)
	(JNIEnv *env, jclass that, jlong arg0)
{
	ComparatorJNI_NATIVE_ENTER(env, that, ComparatorJNI_delete_FUNC);
	delete (JNIComparator *)(intptr_t)arg0;
	ComparatorJNI_NATIVE_EXIT(env, that, ComparatorJNI_delete_FUNC);
}

extern "C" JNIEXPORT void JNICALL ComparatorJNI_NATIVE(init)(JNIEnv *env, jclass that)
{
	env->SetStaticIntField(that, env->GetStaticFieldID(that, "SIZEOF", "I"), (jint)sizeof(struct JNIComparator));
	env->SetStaticLongField(that, env->GetStaticFieldID(that, "BYTEWISE_COMPARATOR", "J"), (jlong)(intptr_t)leveldb::BytewiseComparator());
   return;
}
extern "C" JNIEXPORT void JNICALL ComparatorJNI_NATIVE(memmove__JLorg_fusesource_leveldbjni_internal_NativeComparator_00024ComparatorJNI_2J)(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jlong arg2);
JNIEXPORT void JNICALL ComparatorJNI_NATIVE(memmove__JLorg_fusesource_leveldbjni_internal_NativeComparator_00024ComparatorJNI_2J)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jlong arg2)
{
	struct JNIComparator _arg1, *lparg1=NULL;
	ComparatorJNI_NATIVE_ENTER(env, that, ComparatorJNI_memmove__JLorg_fusesource_leveldbjni_internal_NativeComparator_00024ComparatorJNI_2J_FUNC);
	if (arg1) if ((lparg1 = getComparatorJNIFields(env, arg1, &_arg1)) == NULL) goto fail;
	memmove((void *)(intptr_t)arg0, (const void *)lparg1, (size_t)arg2);
fail:
	ComparatorJNI_NATIVE_EXIT(env, that, ComparatorJNI_memmove__JLorg_fusesource_leveldbjni_internal_NativeComparator_00024ComparatorJNI_2J_FUNC);
}

extern "C" JNIEXPORT void JNICALL ComparatorJNI_NATIVE(memmove__Lorg_fusesource_leveldbjni_internal_NativeComparator_00024ComparatorJNI_2JJ)(JNIEnv *env, jclass that, jobject arg0, jlong arg1, jlong arg2);
JNIEXPORT void JNICALL ComparatorJNI_NATIVE(memmove__Lorg_fusesource_leveldbjni_internal_NativeComparator_00024ComparatorJNI_2JJ)
	(JNIEnv *env, jclass that, jobject arg0, jlong arg1, jlong arg2)
{
	struct JNIComparator _arg0, *lparg0=NULL;
	ComparatorJNI_NATIVE_ENTER(env, that, ComparatorJNI_memmove__Lorg_fusesource_leveldbjni_internal_NativeComparator_00024ComparatorJNI_2JJ_FUNC);
	if (arg0) if ((lparg0 = &_arg0) == NULL) goto fail;
	memmove((void *)lparg0, (const void *)(intptr_t)arg1, (size_t)arg2);
fail:
	if (arg0 && lparg0) setComparatorJNIFields(env, arg0, lparg0);
	ComparatorJNI_NATIVE_EXIT(env, that, ComparatorJNI_memmove__Lorg_fusesource_leveldbjni_internal_NativeComparator_00024ComparatorJNI_2JJ_FUNC);
}

#define DBJNI_NATIVE(func) Java_org_fusesource_leveldbjni_internal_NativeDB_00024DBJNI_##func

/*
extern "C" JNIEXPORT void JNICALL DBJNI_NATIVE(CompactRange)(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jobject arg2);
JNIEXPORT void JNICALL DBJNI_NATIVE(CompactRange)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jobject arg2)
{
	struct leveldb::Slice _arg1, *lparg1=NULL;
	struct leveldb::Slice _arg2, *lparg2=NULL;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_CompactRange_FUNC);
	if (arg1) if ((lparg1 = getNativeSliceFields(env, arg1, &_arg1)) == NULL) goto fail;
	if (arg2) if ((lparg2 = getNativeSliceFields(env, arg2, &_arg2)) == NULL) goto fail;
	((leveldb::DB *)(intptr_t)arg0)->CompactRange(lparg1, lparg2);
fail:
	DBJNI_NATIVE_EXIT(env, that, DBJNI_CompactRange_FUNC);
}
*/

extern "C" JNIEXPORT jlong JNICALL DBJNI_NATIVE(Delete)(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jobject arg2);
JNIEXPORT jlong JNICALL DBJNI_NATIVE(Delete)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jobject arg2)
{
	struct leveldb::WriteOptions _arg1, *lparg1=NULL;
	struct leveldb::Slice _arg2, *lparg2=NULL;
	jlong rc = 0;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_Delete_FUNC);
	if (arg1) if ((lparg1 = getNativeWriteOptionsFields(env, arg1, &_arg1)) == NULL) goto fail;
	if (arg2) if ((lparg2 = getNativeSliceFields(env, arg2, &_arg2)) == NULL) goto fail;
	{		leveldb::Status temp = ((leveldb::DB *)(intptr_t)arg0)->Delete(*lparg1, *lparg2);
		{
			leveldb::Status* copy = new leveldb::Status();
			*copy = temp;
			rc = (jlong)copy;
		}
	}
fail:
	DBJNI_NATIVE_EXIT(env, that, DBJNI_Delete_FUNC);
	return rc;
}

extern "C" JNIEXPORT void JNICALL DBJNI_NATIVE(DeleteGlobalRef)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL DBJNI_NATIVE(DeleteGlobalRef)
	(JNIEnv *env, jclass that, jlong arg0)
{
	DBJNI_NATIVE_ENTER(env, that, DBJNI_DeleteGlobalRef_FUNC);
	env->DeleteGlobalRef((jobject)(intptr_t)arg0);
	DBJNI_NATIVE_EXIT(env, that, DBJNI_DeleteGlobalRef_FUNC);
}

extern "C" JNIEXPORT jlong JNICALL DBJNI_NATIVE(DestroyDB)(JNIEnv *env, jclass that, jstring arg0, jobject arg1);
JNIEXPORT jlong JNICALL DBJNI_NATIVE(DestroyDB)
	(JNIEnv *env, jclass that, jstring arg0, jobject arg1)
{
	const char *lparg0= NULL;
	struct leveldb::Options _arg1, *lparg1=NULL;
	jlong rc = 0;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_DestroyDB_FUNC);
	if (arg0) if ((lparg0 = env->GetStringUTFChars(arg0, NULL)) == NULL) goto fail;
	if (arg1) if ((lparg1 = getNativeOptionsFields(env, arg1, &_arg1)) == NULL) goto fail;
	{		leveldb::Status temp = leveldb::DestroyDB((const char*)lparg0, *lparg1);
		{
			leveldb::Status* copy = new leveldb::Status();
			*copy = temp;
			rc = (jlong)copy;
		}
	}
fail:
	if (arg0 && lparg0) env->ReleaseStringUTFChars(arg0, lparg0);
	DBJNI_NATIVE_EXIT(env, that, DBJNI_DestroyDB_FUNC);
	return rc;
}

extern "C" JNIEXPORT jlong JNICALL DBJNI_NATIVE(Get)(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jobject arg2, jlong arg3);
JNIEXPORT jlong JNICALL DBJNI_NATIVE(Get)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jobject arg2, jlong arg3)
{
	struct leveldb::ReadOptions _arg1, *lparg1=NULL;
	struct leveldb::Slice _arg2, *lparg2=NULL;
	jlong rc = 0;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_Get_FUNC);
	if (arg1) if ((lparg1 = getNativeReadOptionsFields(env, arg1, &_arg1)) == NULL) goto fail;
	if (arg2) if ((lparg2 = getNativeSliceFields(env, arg2, &_arg2)) == NULL) goto fail;
	{		leveldb::Status temp = ((leveldb::DB *)(intptr_t)arg0)->Get(*lparg1, *lparg2, (std::string *)(intptr_t)arg3);
		{
			leveldb::Status* copy = new leveldb::Status();
			*copy = temp;
			rc = (jlong)copy;
		}
	}
fail:
	DBJNI_NATIVE_EXIT(env, that, DBJNI_Get_FUNC);
	return rc;
}

extern "C" JNIEXPORT void JNICALL DBJNI_NATIVE(GetApproximateSizes)(JNIEnv *env, jclass that, jlong arg0, jlong arg1, jint arg2, jlongArray arg3);
JNIEXPORT void JNICALL DBJNI_NATIVE(GetApproximateSizes)
	(JNIEnv *env, jclass that, jlong arg0, jlong arg1, jint arg2, jlongArray arg3)
{
	jlong *lparg3=NULL;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_GetApproximateSizes_FUNC);
	if (arg3) if ((lparg3 = env->GetLongArrayElements(arg3, NULL)) == NULL) goto fail;
	((leveldb::DB *)(intptr_t)arg0)->GetApproximateSizes((const leveldb::Range *)(intptr_t)arg1, arg2, (uint64_t*)lparg3);
fail:
	if (arg3 && lparg3) env->ReleaseLongArrayElements(arg3, lparg3, 0);
	DBJNI_NATIVE_EXIT(env, that, DBJNI_GetApproximateSizes_FUNC);
}

extern "C" JNIEXPORT jlong JNICALL DBJNI_NATIVE(GetMethodID)(JNIEnv *env, jclass that, jclass arg0, jstring arg1, jstring arg2);
JNIEXPORT jlong JNICALL DBJNI_NATIVE(GetMethodID)
	(JNIEnv *env, jclass that, jclass arg0, jstring arg1, jstring arg2)
{
	const char *lparg1= NULL;
	const char *lparg2= NULL;
	jlong rc = 0;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_GetMethodID_FUNC);
	if (arg1) if ((lparg1 = env->GetStringUTFChars(arg1, NULL)) == NULL) goto fail;
	if (arg2) if ((lparg2 = env->GetStringUTFChars(arg2, NULL)) == NULL) goto fail;
	rc = (intptr_t)(jmethodID)env->GetMethodID((jclass)(intptr_t)arg0, lparg1, lparg2);
fail:
	if (arg2 && lparg2) env->ReleaseStringUTFChars(arg2, lparg2);
	if (arg1 && lparg1) env->ReleaseStringUTFChars(arg1, lparg1);
	DBJNI_NATIVE_EXIT(env, that, DBJNI_GetMethodID_FUNC);
	return rc;
}

extern "C" JNIEXPORT jboolean JNICALL DBJNI_NATIVE(GetProperty)(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jlong arg2);
JNIEXPORT jboolean JNICALL DBJNI_NATIVE(GetProperty)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jlong arg2)
{
	struct leveldb::Slice _arg1, *lparg1=NULL;
	jboolean rc = 0;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_GetProperty_FUNC);
	if (arg1) if ((lparg1 = getNativeSliceFields(env, arg1, &_arg1)) == NULL) goto fail;
	rc = (jboolean)((leveldb::DB *)(intptr_t)arg0)->GetProperty(*lparg1, (std::string *)(intptr_t)arg2);
fail:
	DBJNI_NATIVE_EXIT(env, that, DBJNI_GetProperty_FUNC);
	return rc;
}

extern "C" JNIEXPORT jlong JNICALL DBJNI_NATIVE(GetSnapshot)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT jlong JNICALL DBJNI_NATIVE(GetSnapshot)
	(JNIEnv *env, jclass that, jlong arg0)
{
	jlong rc = 0;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_GetSnapshot_FUNC);
	rc = (intptr_t)(leveldb::Snapshot *)((leveldb::DB *)(intptr_t)arg0)->GetSnapshot();
	DBJNI_NATIVE_EXIT(env, that, DBJNI_GetSnapshot_FUNC);
	return rc;
}

extern "C" JNIEXPORT jlong JNICALL DBJNI_NATIVE(NewGlobalRef)(JNIEnv *env, jclass that, jobject arg0);
JNIEXPORT jlong JNICALL DBJNI_NATIVE(NewGlobalRef)
	(JNIEnv *env, jclass that, jobject arg0)
{
	jlong rc = 0;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_NewGlobalRef_FUNC);
	rc = (intptr_t)(jobject)env->NewGlobalRef(arg0);
	DBJNI_NATIVE_EXIT(env, that, DBJNI_NewGlobalRef_FUNC);
	return rc;
}

extern "C" JNIEXPORT jlong JNICALL DBJNI_NATIVE(NewIterator)(JNIEnv *env, jclass that, jlong arg0, jobject arg1);
JNIEXPORT jlong JNICALL DBJNI_NATIVE(NewIterator)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1)
{
	struct leveldb::ReadOptions _arg1, *lparg1=NULL;
	jlong rc = 0;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_NewIterator_FUNC);
	if (arg1) if ((lparg1 = getNativeReadOptionsFields(env, arg1, &_arg1)) == NULL) goto fail;
	rc = (intptr_t)(leveldb::Iterator *)((leveldb::DB *)(intptr_t)arg0)->NewIterator(*lparg1);
fail:
	DBJNI_NATIVE_EXIT(env, that, DBJNI_NewIterator_FUNC);
	return rc;
}

extern "C" JNIEXPORT jlong JNICALL DBJNI_NATIVE(Open)(JNIEnv *env, jclass that, jobject arg0, jstring arg1, jlongArray arg2);
JNIEXPORT jlong JNICALL DBJNI_NATIVE(Open)
	(JNIEnv *env, jclass that, jobject arg0, jstring arg1, jlongArray arg2)
{
	struct leveldb::Options _arg0, *lparg0=NULL;
	const char *lparg1= NULL;
	jlong *lparg2=NULL;
	jlong rc = 0;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_Open_FUNC);
	if (arg0) if ((lparg0 = getNativeOptionsFields(env, arg0, &_arg0)) == NULL) goto fail;
	if (arg1) if ((lparg1 = env->GetStringUTFChars(arg1, NULL)) == NULL) goto fail;
	if (arg2) if ((lparg2 = env->GetLongArrayElements(arg2, NULL)) == NULL) goto fail;
	{		leveldb::Status temp = leveldb::DB::Open(*lparg0, (const char*)lparg1, (leveldb::DB**)lparg2);
		{
			leveldb::Status* copy = new leveldb::Status();
			*copy = temp;
			rc = (jlong)copy;
		}
	}
fail:
	if (arg2 && lparg2) env->ReleaseLongArrayElements(arg2, lparg2, 0);
	if (arg1 && lparg1) env->ReleaseStringUTFChars(arg1, lparg1);
	DBJNI_NATIVE_EXIT(env, that, DBJNI_Open_FUNC);
	return rc;
}

extern "C" JNIEXPORT jlong JNICALL DBJNI_NATIVE(Put)(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jobject arg2, jobject arg3);
JNIEXPORT jlong JNICALL DBJNI_NATIVE(Put)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jobject arg2, jobject arg3)
{
	struct leveldb::WriteOptions _arg1, *lparg1=NULL;
	struct leveldb::Slice _arg2, *lparg2=NULL;
	struct leveldb::Slice _arg3, *lparg3=NULL;
	jlong rc = 0;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_Put_FUNC);
	if (arg1) if ((lparg1 = getNativeWriteOptionsFields(env, arg1, &_arg1)) == NULL) goto fail;
	if (arg2) if ((lparg2 = getNativeSliceFields(env, arg2, &_arg2)) == NULL) goto fail;
	if (arg3) if ((lparg3 = getNativeSliceFields(env, arg3, &_arg3)) == NULL) goto fail;
	{		leveldb::Status temp = ((leveldb::DB *)(intptr_t)arg0)->Put(*lparg1, *lparg2, *lparg3);
		{
			leveldb::Status* copy = new leveldb::Status();
			*copy = temp;
			rc = (jlong)copy;
		}
	}
fail:
	DBJNI_NATIVE_EXIT(env, that, DBJNI_Put_FUNC);
	return rc;
}

extern "C" JNIEXPORT void JNICALL DBJNI_NATIVE(ReleaseSnapshot)(JNIEnv *env, jclass that, jlong arg0, jlong arg1);
JNIEXPORT void JNICALL DBJNI_NATIVE(ReleaseSnapshot)
	(JNIEnv *env, jclass that, jlong arg0, jlong arg1)
{
	DBJNI_NATIVE_ENTER(env, that, DBJNI_ReleaseSnapshot_FUNC);
	((leveldb::DB *)(intptr_t)arg0)->ReleaseSnapshot((const leveldb::Snapshot *)(intptr_t)arg1);
	DBJNI_NATIVE_EXIT(env, that, DBJNI_ReleaseSnapshot_FUNC);
}

extern "C" JNIEXPORT jlong JNICALL DBJNI_NATIVE(RepairDB)(JNIEnv *env, jclass that, jstring arg0, jobject arg1);
JNIEXPORT jlong JNICALL DBJNI_NATIVE(RepairDB)
	(JNIEnv *env, jclass that, jstring arg0, jobject arg1)
{
	const char *lparg0= NULL;
	struct leveldb::Options _arg1, *lparg1=NULL;
	jlong rc = 0;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_RepairDB_FUNC);
	if (arg0) if ((lparg0 = env->GetStringUTFChars(arg0, NULL)) == NULL) goto fail;
	if (arg1) if ((lparg1 = getNativeOptionsFields(env, arg1, &_arg1)) == NULL) goto fail;
	{		leveldb::Status temp = leveldb::RepairDB((const char*)lparg0, *lparg1);
		{
			leveldb::Status* copy = new leveldb::Status();
			*copy = temp;
			rc = (jlong)copy;
		}
	}
fail:
	if (arg0 && lparg0) env->ReleaseStringUTFChars(arg0, lparg0);
	DBJNI_NATIVE_EXIT(env, that, DBJNI_RepairDB_FUNC);
	return rc;
}

/*
extern "C" JNIEXPORT void JNICALL DBJNI_NATIVE(ResumeCompactions)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL DBJNI_NATIVE(ResumeCompactions)
	(JNIEnv *env, jclass that, jlong arg0)
{
	DBJNI_NATIVE_ENTER(env, that, DBJNI_ResumeCompactions_FUNC);
	((leveldb::DB *)(intptr_t)arg0)->ResumeCompactions();
	DBJNI_NATIVE_EXIT(env, that, DBJNI_ResumeCompactions_FUNC);
}
*/

/*
extern "C" JNIEXPORT void JNICALL DBJNI_NATIVE(SuspendCompactions)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL DBJNI_NATIVE(SuspendCompactions)
	(JNIEnv *env, jclass that, jlong arg0)
{
	DBJNI_NATIVE_ENTER(env, that, DBJNI_SuspendCompactions_FUNC);
	((leveldb::DB *)(intptr_t)arg0)->SuspendCompactions();
	DBJNI_NATIVE_EXIT(env, that, DBJNI_SuspendCompactions_FUNC);
}
*/

extern "C" JNIEXPORT jlong JNICALL DBJNI_NATIVE(Write)(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jlong arg2);
JNIEXPORT jlong JNICALL DBJNI_NATIVE(Write)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jlong arg2)
{
	struct leveldb::WriteOptions _arg1, *lparg1=NULL;
	jlong rc = 0;
	DBJNI_NATIVE_ENTER(env, that, DBJNI_Write_FUNC);
	if (arg1) if ((lparg1 = getNativeWriteOptionsFields(env, arg1, &_arg1)) == NULL) goto fail;
	{		leveldb::Status temp = ((leveldb::DB *)(intptr_t)arg0)->Write(*lparg1, (leveldb::WriteBatch *)(intptr_t)arg2);
		{
			leveldb::Status* copy = new leveldb::Status();
			*copy = temp;
			rc = (jlong)copy;
		}
	}
fail:
	if (arg1 && lparg1) setNativeWriteOptionsFields(env, arg1, lparg1);
	DBJNI_NATIVE_EXIT(env, that, DBJNI_Write_FUNC);
	return rc;
}

extern "C" JNIEXPORT void JNICALL DBJNI_NATIVE(delete)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL DBJNI_NATIVE(delete)
	(JNIEnv *env, jclass that, jlong arg0)
{
	DBJNI_NATIVE_ENTER(env, that, DBJNI_delete_FUNC);
	delete (leveldb::DB *)(intptr_t)arg0;
	DBJNI_NATIVE_EXIT(env, that, DBJNI_delete_FUNC);
}

#define IteratorJNI_NATIVE(func) Java_org_fusesource_leveldbjni_internal_NativeIterator_00024IteratorJNI_##func

extern "C" JNIEXPORT void JNICALL IteratorJNI_NATIVE(Next)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL IteratorJNI_NATIVE(Next)
	(JNIEnv *env, jclass that, jlong arg0)
{
	IteratorJNI_NATIVE_ENTER(env, that, IteratorJNI_Next_FUNC);
	((leveldb::Iterator *)(intptr_t)arg0)->Next();
	IteratorJNI_NATIVE_EXIT(env, that, IteratorJNI_Next_FUNC);
}

extern "C" JNIEXPORT void JNICALL IteratorJNI_NATIVE(Prev)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL IteratorJNI_NATIVE(Prev)
	(JNIEnv *env, jclass that, jlong arg0)
{
	IteratorJNI_NATIVE_ENTER(env, that, IteratorJNI_Prev_FUNC);
	((leveldb::Iterator *)(intptr_t)arg0)->Prev();
	IteratorJNI_NATIVE_EXIT(env, that, IteratorJNI_Prev_FUNC);
}

extern "C" JNIEXPORT void JNICALL IteratorJNI_NATIVE(Seek)(JNIEnv *env, jclass that, jlong arg0, jobject arg1);
JNIEXPORT void JNICALL IteratorJNI_NATIVE(Seek)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1)
{
	struct leveldb::Slice _arg1, *lparg1=NULL;
	IteratorJNI_NATIVE_ENTER(env, that, IteratorJNI_Seek_FUNC);
	if (arg1) if ((lparg1 = getNativeSliceFields(env, arg1, &_arg1)) == NULL) goto fail;
	((leveldb::Iterator *)(intptr_t)arg0)->Seek(*lparg1);
fail:
	IteratorJNI_NATIVE_EXIT(env, that, IteratorJNI_Seek_FUNC);
}

extern "C" JNIEXPORT void JNICALL IteratorJNI_NATIVE(SeekToFirst)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL IteratorJNI_NATIVE(SeekToFirst)
	(JNIEnv *env, jclass that, jlong arg0)
{
	IteratorJNI_NATIVE_ENTER(env, that, IteratorJNI_SeekToFirst_FUNC);
	((leveldb::Iterator *)(intptr_t)arg0)->SeekToFirst();
	IteratorJNI_NATIVE_EXIT(env, that, IteratorJNI_SeekToFirst_FUNC);
}

extern "C" JNIEXPORT void JNICALL IteratorJNI_NATIVE(SeekToLast)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL IteratorJNI_NATIVE(SeekToLast)
	(JNIEnv *env, jclass that, jlong arg0)
{
	IteratorJNI_NATIVE_ENTER(env, that, IteratorJNI_SeekToLast_FUNC);
	((leveldb::Iterator *)(intptr_t)arg0)->SeekToLast();
	IteratorJNI_NATIVE_EXIT(env, that, IteratorJNI_SeekToLast_FUNC);
}

extern "C" JNIEXPORT jboolean JNICALL IteratorJNI_NATIVE(Valid)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT jboolean JNICALL IteratorJNI_NATIVE(Valid)
	(JNIEnv *env, jclass that, jlong arg0)
{
	jboolean rc = 0;
	IteratorJNI_NATIVE_ENTER(env, that, IteratorJNI_Valid_FUNC);
	rc = (jboolean)((leveldb::Iterator *)(intptr_t)arg0)->Valid();
	IteratorJNI_NATIVE_EXIT(env, that, IteratorJNI_Valid_FUNC);
	return rc;
}

extern "C" JNIEXPORT void JNICALL IteratorJNI_NATIVE(delete)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL IteratorJNI_NATIVE(delete)
	(JNIEnv *env, jclass that, jlong arg0)
{
	IteratorJNI_NATIVE_ENTER(env, that, IteratorJNI_delete_FUNC);
	delete (leveldb::Iterator *)(intptr_t)arg0;
	IteratorJNI_NATIVE_EXIT(env, that, IteratorJNI_delete_FUNC);
}

extern "C" JNIEXPORT jlong JNICALL IteratorJNI_NATIVE(key)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT jlong JNICALL IteratorJNI_NATIVE(key)
	(JNIEnv *env, jclass that, jlong arg0)
{
	jlong rc = 0;
	IteratorJNI_NATIVE_ENTER(env, that, IteratorJNI_key_FUNC);
	{		leveldb::Slice temp = ((leveldb::Iterator *)(intptr_t)arg0)->key();
		{
			leveldb::Slice* copy = new leveldb::Slice();
			*copy = temp;
			rc = (jlong)copy;
		}
	}
	IteratorJNI_NATIVE_EXIT(env, that, IteratorJNI_key_FUNC);
	return rc;
}

extern "C" JNIEXPORT jlong JNICALL IteratorJNI_NATIVE(status)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT jlong JNICALL IteratorJNI_NATIVE(status)
	(JNIEnv *env, jclass that, jlong arg0)
{
	jlong rc = 0;
	IteratorJNI_NATIVE_ENTER(env, that, IteratorJNI_status_FUNC);
	{		leveldb::Status temp = ((leveldb::Iterator *)(intptr_t)arg0)->status();
		{
			leveldb::Status* copy = new leveldb::Status();
			*copy = temp;
			rc = (jlong)copy;
		}
	}
	IteratorJNI_NATIVE_EXIT(env, that, IteratorJNI_status_FUNC);
	return rc;
}

extern "C" JNIEXPORT jlong JNICALL IteratorJNI_NATIVE(value)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT jlong JNICALL IteratorJNI_NATIVE(value)
	(JNIEnv *env, jclass that, jlong arg0)
{
	jlong rc = 0;
	IteratorJNI_NATIVE_ENTER(env, that, IteratorJNI_value_FUNC);
	{		leveldb::Slice temp = ((leveldb::Iterator *)(intptr_t)arg0)->value();
		{
			leveldb::Slice* copy = new leveldb::Slice();
			*copy = temp;
			rc = (jlong)copy;
		}
	}
	IteratorJNI_NATIVE_EXIT(env, that, IteratorJNI_value_FUNC);
	return rc;
}

#define LoggerJNI_NATIVE(func) Java_org_fusesource_leveldbjni_internal_NativeLogger_00024LoggerJNI_##func

extern "C" JNIEXPORT jlong JNICALL LoggerJNI_NATIVE(create)(JNIEnv *env, jclass that);
JNIEXPORT jlong JNICALL LoggerJNI_NATIVE(create)
	(JNIEnv *env, jclass that)
{
	jlong rc = 0;
	LoggerJNI_NATIVE_ENTER(env, that, LoggerJNI_create_FUNC);
	rc = (intptr_t)(JNILogger *)new JNILogger();
	LoggerJNI_NATIVE_EXIT(env, that, LoggerJNI_create_FUNC);
	return rc;
}

extern "C" JNIEXPORT void JNICALL LoggerJNI_NATIVE(delete)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL LoggerJNI_NATIVE(delete)
	(JNIEnv *env, jclass that, jlong arg0)
{
	LoggerJNI_NATIVE_ENTER(env, that, LoggerJNI_delete_FUNC);
	delete (JNILogger *)(intptr_t)arg0;
	LoggerJNI_NATIVE_EXIT(env, that, LoggerJNI_delete_FUNC);
}

extern "C" JNIEXPORT void JNICALL LoggerJNI_NATIVE(init)(JNIEnv *env, jclass that)
{
	env->SetStaticIntField(that, env->GetStaticFieldID(that, "SIZEOF", "I"), (jint)sizeof(struct JNILogger));
   return;
}
extern "C" JNIEXPORT void JNICALL LoggerJNI_NATIVE(memmove)(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jlong arg2);
JNIEXPORT void JNICALL LoggerJNI_NATIVE(memmove)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jlong arg2)
{
	struct JNILogger _arg1, *lparg1=NULL;
	LoggerJNI_NATIVE_ENTER(env, that, LoggerJNI_memmove_FUNC);
	if (arg1) if ((lparg1 = getLoggerJNIFields(env, arg1, &_arg1)) == NULL) goto fail;
	memmove((void *)(intptr_t)arg0, (const void *)lparg1, (size_t)arg2);
fail:
	LoggerJNI_NATIVE_EXIT(env, that, LoggerJNI_memmove_FUNC);
}

#define NativeOptions_NATIVE(func) Java_org_fusesource_leveldbjni_internal_NativeOptions_##func

extern "C" JNIEXPORT void JNICALL NativeOptions_NATIVE(init)(JNIEnv *env, jclass that)
{
	env->SetStaticLongField(that, env->GetStaticFieldID(that, "DEFAULT_ENV", "J"), (jlong)(intptr_t)leveldb::Env::Default());
   return;
}
#define RangeJNI_NATIVE(func) Java_org_fusesource_leveldbjni_internal_NativeRange_00024RangeJNI_##func

extern "C" JNIEXPORT void JNICALL RangeJNI_NATIVE(init)(JNIEnv *env, jclass that)
{
	env->SetStaticIntField(that, env->GetStaticFieldID(that, "SIZEOF", "I"), (jint)sizeof(struct leveldb::Range));
   return;
}
extern "C" JNIEXPORT void JNICALL RangeJNI_NATIVE(memmove__JLorg_fusesource_leveldbjni_internal_NativeRange_00024RangeJNI_2J)(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jlong arg2);
JNIEXPORT void JNICALL RangeJNI_NATIVE(memmove__JLorg_fusesource_leveldbjni_internal_NativeRange_00024RangeJNI_2J)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jlong arg2)
{
	struct leveldb::Range _arg1, *lparg1=NULL;
	RangeJNI_NATIVE_ENTER(env, that, RangeJNI_memmove__JLorg_fusesource_leveldbjni_internal_NativeRange_00024RangeJNI_2J_FUNC);
	if (arg1) if ((lparg1 = getRangeJNIFields(env, arg1, &_arg1)) == NULL) goto fail;
	memmove((void *)(intptr_t)arg0, (const void *)lparg1, (size_t)arg2);
fail:
	RangeJNI_NATIVE_EXIT(env, that, RangeJNI_memmove__JLorg_fusesource_leveldbjni_internal_NativeRange_00024RangeJNI_2J_FUNC);
}

extern "C" JNIEXPORT void JNICALL RangeJNI_NATIVE(memmove__Lorg_fusesource_leveldbjni_internal_NativeRange_00024RangeJNI_2JJ)(JNIEnv *env, jclass that, jobject arg0, jlong arg1, jlong arg2);
JNIEXPORT void JNICALL RangeJNI_NATIVE(memmove__Lorg_fusesource_leveldbjni_internal_NativeRange_00024RangeJNI_2JJ)
	(JNIEnv *env, jclass that, jobject arg0, jlong arg1, jlong arg2)
{
	struct leveldb::Range _arg0, *lparg0=NULL;
	RangeJNI_NATIVE_ENTER(env, that, RangeJNI_memmove__Lorg_fusesource_leveldbjni_internal_NativeRange_00024RangeJNI_2JJ_FUNC);
	if (arg0) if ((lparg0 = &_arg0) == NULL) goto fail;
	memmove((void *)lparg0, (const void *)(intptr_t)arg1, (size_t)arg2);
fail:
	if (arg0 && lparg0) setRangeJNIFields(env, arg0, lparg0);
	RangeJNI_NATIVE_EXIT(env, that, RangeJNI_memmove__Lorg_fusesource_leveldbjni_internal_NativeRange_00024RangeJNI_2JJ_FUNC);
}

#define SliceJNI_NATIVE(func) Java_org_fusesource_leveldbjni_internal_NativeSlice_00024SliceJNI_##func

extern "C" JNIEXPORT void JNICALL SliceJNI_NATIVE(delete)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL SliceJNI_NATIVE(delete)
	(JNIEnv *env, jclass that, jlong arg0)
{
	SliceJNI_NATIVE_ENTER(env, that, SliceJNI_delete_FUNC);
	delete (leveldb::Slice *)(intptr_t)arg0;
	SliceJNI_NATIVE_EXIT(env, that, SliceJNI_delete_FUNC);
}

extern "C" JNIEXPORT void JNICALL SliceJNI_NATIVE(init)(JNIEnv *env, jclass that)
{
	env->SetStaticIntField(that, env->GetStaticFieldID(that, "SIZEOF", "I"), (jint)sizeof(struct leveldb::Slice));
   return;
}
extern "C" JNIEXPORT void JNICALL SliceJNI_NATIVE(memmove__JLorg_fusesource_leveldbjni_internal_NativeSlice_2J)(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jlong arg2);
JNIEXPORT void JNICALL SliceJNI_NATIVE(memmove__JLorg_fusesource_leveldbjni_internal_NativeSlice_2J)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jlong arg2)
{
	struct leveldb::Slice _arg1, *lparg1=NULL;
	SliceJNI_NATIVE_ENTER(env, that, SliceJNI_memmove__JLorg_fusesource_leveldbjni_internal_NativeSlice_2J_FUNC);
	if (arg1) if ((lparg1 = getNativeSliceFields(env, arg1, &_arg1)) == NULL) goto fail;
	memmove((void *)(intptr_t)arg0, (const void *)lparg1, (size_t)arg2);
fail:
	SliceJNI_NATIVE_EXIT(env, that, SliceJNI_memmove__JLorg_fusesource_leveldbjni_internal_NativeSlice_2J_FUNC);
}

extern "C" JNIEXPORT void JNICALL SliceJNI_NATIVE(memmove__Lorg_fusesource_leveldbjni_internal_NativeSlice_2JJ)(JNIEnv *env, jclass that, jobject arg0, jlong arg1, jlong arg2);
JNIEXPORT void JNICALL SliceJNI_NATIVE(memmove__Lorg_fusesource_leveldbjni_internal_NativeSlice_2JJ)
	(JNIEnv *env, jclass that, jobject arg0, jlong arg1, jlong arg2)
{
	struct leveldb::Slice _arg0, *lparg0=NULL;
	SliceJNI_NATIVE_ENTER(env, that, SliceJNI_memmove__Lorg_fusesource_leveldbjni_internal_NativeSlice_2JJ_FUNC);
	if (arg0) if ((lparg0 = &_arg0) == NULL) goto fail;
	memmove((void *)lparg0, (const void *)(intptr_t)arg1, (size_t)arg2);
fail:
	if (arg0 && lparg0) setNativeSliceFields(env, arg0, lparg0);
	SliceJNI_NATIVE_EXIT(env, that, SliceJNI_memmove__Lorg_fusesource_leveldbjni_internal_NativeSlice_2JJ_FUNC);
}

#define StatusJNI_NATIVE(func) Java_org_fusesource_leveldbjni_internal_NativeStatus_00024StatusJNI_##func

extern "C" JNIEXPORT jboolean JNICALL StatusJNI_NATIVE(IsNotFound)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT jboolean JNICALL StatusJNI_NATIVE(IsNotFound)
	(JNIEnv *env, jclass that, jlong arg0)
{
	jboolean rc = 0;
	StatusJNI_NATIVE_ENTER(env, that, StatusJNI_IsNotFound_FUNC);
	rc = (jboolean)((leveldb::Status *)(intptr_t)arg0)->IsNotFound();
	StatusJNI_NATIVE_EXIT(env, that, StatusJNI_IsNotFound_FUNC);
	return rc;
}

extern "C" JNIEXPORT jlong JNICALL StatusJNI_NATIVE(ToString)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT jlong JNICALL StatusJNI_NATIVE(ToString)
	(JNIEnv *env, jclass that, jlong arg0)
{
	jlong rc = 0;
	StatusJNI_NATIVE_ENTER(env, that, StatusJNI_ToString_FUNC);
	{		std::string temp = ((leveldb::Status *)(intptr_t)arg0)->ToString();
		{
			std::string* copy = new std::string();
			*copy = temp;
			rc = (jlong)copy;
		}
	}
	StatusJNI_NATIVE_EXIT(env, that, StatusJNI_ToString_FUNC);
	return rc;
}

extern "C" JNIEXPORT void JNICALL StatusJNI_NATIVE(delete)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL StatusJNI_NATIVE(delete)
	(JNIEnv *env, jclass that, jlong arg0)
{
	StatusJNI_NATIVE_ENTER(env, that, StatusJNI_delete_FUNC);
	delete (leveldb::Status *)(intptr_t)arg0;
	StatusJNI_NATIVE_EXIT(env, that, StatusJNI_delete_FUNC);
}

extern "C" JNIEXPORT jboolean JNICALL StatusJNI_NATIVE(ok)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT jboolean JNICALL StatusJNI_NATIVE(ok)
	(JNIEnv *env, jclass that, jlong arg0)
{
	jboolean rc = 0;
	StatusJNI_NATIVE_ENTER(env, that, StatusJNI_ok_FUNC);
	rc = (jboolean)((leveldb::Status *)(intptr_t)arg0)->ok();
	StatusJNI_NATIVE_EXIT(env, that, StatusJNI_ok_FUNC);
	return rc;
}

#define StdStringJNI_NATIVE(func) Java_org_fusesource_leveldbjni_internal_NativeStdString_00024StdStringJNI_##func

extern "C" JNIEXPORT jlong JNICALL StdStringJNI_NATIVE(c_1str_1ptr)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT jlong JNICALL StdStringJNI_NATIVE(c_1str_1ptr)
	(JNIEnv *env, jclass that, jlong arg0)
{
	jlong rc = 0;
	StdStringJNI_NATIVE_ENTER(env, that, StdStringJNI_c_1str_1ptr_FUNC);
	rc = (intptr_t)(const char*)((std::string *)(intptr_t)arg0)->c_str();
	StdStringJNI_NATIVE_EXIT(env, that, StdStringJNI_c_1str_1ptr_FUNC);
	return rc;
}

extern "C" JNIEXPORT jlong JNICALL StdStringJNI_NATIVE(create__)(JNIEnv *env, jclass that);
JNIEXPORT jlong JNICALL StdStringJNI_NATIVE(create__)
	(JNIEnv *env, jclass that)
{
	jlong rc = 0;
	StdStringJNI_NATIVE_ENTER(env, that, StdStringJNI_create___FUNC);
	rc = (intptr_t)(std::string *)new std::string();
	StdStringJNI_NATIVE_EXIT(env, that, StdStringJNI_create___FUNC);
	return rc;
}

extern "C" JNIEXPORT jlong JNICALL StdStringJNI_NATIVE(create__Ljava_lang_String_2)(JNIEnv *env, jclass that, jstring arg0);
JNIEXPORT jlong JNICALL StdStringJNI_NATIVE(create__Ljava_lang_String_2)
	(JNIEnv *env, jclass that, jstring arg0)
{
	const char *lparg0= NULL;
	jlong rc = 0;
	StdStringJNI_NATIVE_ENTER(env, that, StdStringJNI_create__Ljava_lang_String_2_FUNC);
	if (arg0) if ((lparg0 = env->GetStringUTFChars(arg0, NULL)) == NULL) goto fail;
	rc = (intptr_t)(std::string *)new std::string(lparg0);
fail:
	if (arg0 && lparg0) env->ReleaseStringUTFChars(arg0, lparg0);
	StdStringJNI_NATIVE_EXIT(env, that, StdStringJNI_create__Ljava_lang_String_2_FUNC);
	return rc;
}

extern "C" JNIEXPORT void JNICALL StdStringJNI_NATIVE(delete)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL StdStringJNI_NATIVE(delete)
	(JNIEnv *env, jclass that, jlong arg0)
{
	StdStringJNI_NATIVE_ENTER(env, that, StdStringJNI_delete_FUNC);
	delete (std::string *)(intptr_t)arg0;
	StdStringJNI_NATIVE_EXIT(env, that, StdStringJNI_delete_FUNC);
}

extern "C" JNIEXPORT jlong JNICALL StdStringJNI_NATIVE(length)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT jlong JNICALL StdStringJNI_NATIVE(length)
	(JNIEnv *env, jclass that, jlong arg0)
{
	jlong rc = 0;
	StdStringJNI_NATIVE_ENTER(env, that, StdStringJNI_length_FUNC);
	rc = (size_t)((std::string *)(intptr_t)arg0)->length();
	StdStringJNI_NATIVE_EXIT(env, that, StdStringJNI_length_FUNC);
	return rc;
}

#define WriteBatchJNI_NATIVE(func) Java_org_fusesource_leveldbjni_internal_NativeWriteBatch_00024WriteBatchJNI_##func

extern "C" JNIEXPORT void JNICALL WriteBatchJNI_NATIVE(Clear)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL WriteBatchJNI_NATIVE(Clear)
	(JNIEnv *env, jclass that, jlong arg0)
{
	WriteBatchJNI_NATIVE_ENTER(env, that, WriteBatchJNI_Clear_FUNC);
	((leveldb::WriteBatch *)(intptr_t)arg0)->Clear();
	WriteBatchJNI_NATIVE_EXIT(env, that, WriteBatchJNI_Clear_FUNC);
}

extern "C" JNIEXPORT void JNICALL WriteBatchJNI_NATIVE(Delete)(JNIEnv *env, jclass that, jlong arg0, jobject arg1);
JNIEXPORT void JNICALL WriteBatchJNI_NATIVE(Delete)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1)
{
	struct leveldb::Slice _arg1, *lparg1=NULL;
	WriteBatchJNI_NATIVE_ENTER(env, that, WriteBatchJNI_Delete_FUNC);
	if (arg1) if ((lparg1 = getNativeSliceFields(env, arg1, &_arg1)) == NULL) goto fail;
	((leveldb::WriteBatch *)(intptr_t)arg0)->Delete(*lparg1);
fail:
	WriteBatchJNI_NATIVE_EXIT(env, that, WriteBatchJNI_Delete_FUNC);
}

extern "C" JNIEXPORT void JNICALL WriteBatchJNI_NATIVE(Put)(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jobject arg2);
JNIEXPORT void JNICALL WriteBatchJNI_NATIVE(Put)
	(JNIEnv *env, jclass that, jlong arg0, jobject arg1, jobject arg2)
{
	struct leveldb::Slice _arg1, *lparg1=NULL;
	struct leveldb::Slice _arg2, *lparg2=NULL;
	WriteBatchJNI_NATIVE_ENTER(env, that, WriteBatchJNI_Put_FUNC);
	if (arg1) if ((lparg1 = getNativeSliceFields(env, arg1, &_arg1)) == NULL) goto fail;
	if (arg2) if ((lparg2 = getNativeSliceFields(env, arg2, &_arg2)) == NULL) goto fail;
	((leveldb::WriteBatch *)(intptr_t)arg0)->Put(*lparg1, *lparg2);
fail:
	WriteBatchJNI_NATIVE_EXIT(env, that, WriteBatchJNI_Put_FUNC);
}

extern "C" JNIEXPORT jlong JNICALL WriteBatchJNI_NATIVE(create)(JNIEnv *env, jclass that);
JNIEXPORT jlong JNICALL WriteBatchJNI_NATIVE(create)
	(JNIEnv *env, jclass that)
{
	jlong rc = 0;
	WriteBatchJNI_NATIVE_ENTER(env, that, WriteBatchJNI_create_FUNC);
	rc = (intptr_t)(leveldb::WriteBatch *)new leveldb::WriteBatch();
	WriteBatchJNI_NATIVE_EXIT(env, that, WriteBatchJNI_create_FUNC);
	return rc;
}

extern "C" JNIEXPORT void JNICALL WriteBatchJNI_NATIVE(delete)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT void JNICALL WriteBatchJNI_NATIVE(delete)
	(JNIEnv *env, jclass that, jlong arg0)
{
	WriteBatchJNI_NATIVE_ENTER(env, that, WriteBatchJNI_delete_FUNC);
	delete (leveldb::WriteBatch *)(intptr_t)arg0;
	WriteBatchJNI_NATIVE_EXIT(env, that, WriteBatchJNI_delete_FUNC);
}

#define EnvJNI_NATIVE(func) Java_org_fusesource_leveldbjni_internal_Util_00024EnvJNI_##func

extern "C" JNIEXPORT jlong JNICALL EnvJNI_NATIVE(Default)(JNIEnv *env, jclass that);
JNIEXPORT jlong JNICALL EnvJNI_NATIVE(Default)
	(JNIEnv *env, jclass that)
{
	jlong rc = 0;
	EnvJNI_NATIVE_ENTER(env, that, EnvJNI_Default_FUNC);
	rc = (intptr_t)(leveldb::Env *)leveldb::Env::Default();
	EnvJNI_NATIVE_EXIT(env, that, EnvJNI_Default_FUNC);
	return rc;
}

extern "C" JNIEXPORT void JNICALL EnvJNI_NATIVE(Schedule)(JNIEnv *env, jclass that, jlong arg0, jlong arg1, jlong arg2);
JNIEXPORT void JNICALL EnvJNI_NATIVE(Schedule)
	(JNIEnv *env, jclass that, jlong arg0, jlong arg1, jlong arg2)
{
	EnvJNI_NATIVE_ENTER(env, that, EnvJNI_Schedule_FUNC);
	((leveldb::Env *)(intptr_t)arg0)->Schedule((void (*)(void*))arg1, (void *)(intptr_t)arg2);
	EnvJNI_NATIVE_EXIT(env, that, EnvJNI_Schedule_FUNC);
}

#define UtilJNI_NATIVE(func) Java_org_fusesource_leveldbjni_internal_Util_00024UtilJNI_##func

#if defined(_WIN32) || defined(_WIN64)
extern "C" JNIEXPORT jint JNICALL UtilJNI_NATIVE(CreateHardLinkW)(JNIEnv *env, jclass that, jstring arg0, jstring arg1, jlong arg2);
JNIEXPORT jint JNICALL UtilJNI_NATIVE(CreateHardLinkW)
	(JNIEnv *env, jclass that, jstring arg0, jstring arg1, jlong arg2)
{
	const jchar *lparg0= NULL;
	const jchar *lparg1= NULL;
	jint rc = 0;
	UtilJNI_NATIVE_ENTER(env, that, UtilJNI_CreateHardLinkW_FUNC);
	if (arg0) if ((lparg0 = env->GetStringChars(arg0, NULL)) == NULL) goto fail;
	if (arg1) if ((lparg1 = env->GetStringChars(arg1, NULL)) == NULL) goto fail;
	rc = (jint)CreateHardLinkW((LPCWSTR)(intptr_t)lparg0, (LPCWSTR)(intptr_t)lparg1, (LPSECURITY_ATTRIBUTES)(intptr_t)arg2);
fail:
	if (arg1 && lparg1) env->ReleaseStringChars(arg1, lparg1);
	if (arg0 && lparg0) env->ReleaseStringChars(arg0, lparg0);
	UtilJNI_NATIVE_EXIT(env, that, UtilJNI_CreateHardLinkW_FUNC);
	return rc;
}
#endif

/*
extern "C" JNIEXPORT jint JNICALL UtilJNI_NATIVE(errno)(JNIEnv *env, jclass that);
JNIEXPORT jint JNICALL UtilJNI_NATIVE(errno)
	(JNIEnv *env, jclass that)
{
	jint rc = 0;
	UtilJNI_NATIVE_ENTER(env, that, UtilJNI_errno_FUNC);
	rc = (jint)errno;
	UtilJNI_NATIVE_EXIT(env, that, UtilJNI_errno_FUNC);
	return rc;
}
*/

extern "C" JNIEXPORT void JNICALL UtilJNI_NATIVE(init)(JNIEnv *env, jclass that)
{
#if defined(_WIN32) || defined(_WIN64)
	env->SetStaticIntField(that, env->GetStaticFieldID(that, "ON_WINDOWS", "I"), (jint)1);
#endif
   return;
}
#if !defined(_WIN32) && !defined(_WIN64)
extern "C" JNIEXPORT jint JNICALL UtilJNI_NATIVE(link)(JNIEnv *env, jclass that, jstring arg0, jstring arg1);
JNIEXPORT jint JNICALL UtilJNI_NATIVE(link)
	(JNIEnv *env, jclass that, jstring arg0, jstring arg1)
{
	const char *lparg0= NULL;
	const char *lparg1= NULL;
	jint rc = 0;
	UtilJNI_NATIVE_ENTER(env, that, UtilJNI_link_FUNC);
	if (arg0) if ((lparg0 = env->GetStringUTFChars(arg0, NULL)) == NULL) goto fail;
	if (arg1) if ((lparg1 = env->GetStringUTFChars(arg1, NULL)) == NULL) goto fail;
	rc = (jint)link((const char*)lparg0, (const char*)lparg1);
fail:
	if (arg1 && lparg1) env->ReleaseStringUTFChars(arg1, lparg1);
	if (arg0 && lparg0) env->ReleaseStringUTFChars(arg0, lparg0);
	UtilJNI_NATIVE_EXIT(env, that, UtilJNI_link_FUNC);
	return rc;
}
#endif

extern "C" JNIEXPORT jlong JNICALL UtilJNI_NATIVE(strerror)(JNIEnv *env, jclass that, jint arg0);
JNIEXPORT jlong JNICALL UtilJNI_NATIVE(strerror)
	(JNIEnv *env, jclass that, jint arg0)
{
	jlong rc = 0;
	UtilJNI_NATIVE_ENTER(env, that, UtilJNI_strerror_FUNC);
	rc = (intptr_t)(char *)strerror(arg0);
	UtilJNI_NATIVE_EXIT(env, that, UtilJNI_strerror_FUNC);
	return rc;
}

extern "C" JNIEXPORT jint JNICALL UtilJNI_NATIVE(strlen)(JNIEnv *env, jclass that, jlong arg0);
JNIEXPORT jint JNICALL UtilJNI_NATIVE(strlen)
	(JNIEnv *env, jclass that, jlong arg0)
{
	jint rc = 0;
	UtilJNI_NATIVE_ENTER(env, that, UtilJNI_strlen_FUNC);
	rc = (jint)strlen((const char *)(intptr_t)arg0);
	UtilJNI_NATIVE_EXIT(env, that, UtilJNI_strlen_FUNC);
	return rc;
}

