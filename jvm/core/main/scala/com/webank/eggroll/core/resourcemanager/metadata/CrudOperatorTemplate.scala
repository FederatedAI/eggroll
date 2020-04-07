/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.webank.eggroll.core.resourcemanager.metadata
/*import org.apache.ibatis.session.SqlSession



class CrudOperatorTemplate {
  def doCrudOperationSingleResult[T](functor: (T, SqlSession) => T, input: T, sqlSession: SqlSession = null, openTransaction: Boolean = false): T = {
    val isOpenSession = sqlSession == null
    val finalSqlSession = if (isOpenSession) RdbConnectionPool.openSession() else sqlSession

    try {
      functor(input, finalSqlSession)
    } catch {
      case e: Exception =>
        if (openTransaction) {
          finalSqlSession.rollback()
        }
        throw e
    } finally {
      if (isOpenSession) {
        if (openTransaction) {
          finalSqlSession.commit()
        }
        finalSqlSession.close()
      }
    }
  }

  def doCrudOperationMultipleResults[T](functor: (T, SqlSession) => Array[T], input: T, sqlSession: SqlSession = null, openTransaction: Boolean = false): Array[T] = {
    val isOpenSession = sqlSession == null
    val finalSqlSession = if (isOpenSession) RdbConnectionPool.openSession() else sqlSession

    try {
      functor(input, finalSqlSession)
    } catch {
      case e: Exception =>
        if (openTransaction) {
          finalSqlSession.rollback()
        }
        throw e
    } finally {
      if (isOpenSession) {
        if (openTransaction) {
          finalSqlSession.commit()
        }
        finalSqlSession.close()
      }
    }
  }

  def doCrudOperationListInput[T, U](functor: (java.util.List[T], SqlSession) => U, input: java.util.List[T], sqlSession: SqlSession = null, openTransaction: Boolean = false): U = {
    val isOpenSession = sqlSession == null
    val finalSqlSession = if (isOpenSession) RdbConnectionPool.openSession() else sqlSession

    try {
      functor(input, finalSqlSession)
    } catch {
      case e: Exception =>
        if (openTransaction) {
          finalSqlSession.rollback()
        }
        throw e
    } finally {
      if (isOpenSession) {
        if (openTransaction) {
          finalSqlSession.commit()
        }
        finalSqlSession.close()
      }
    }
  }
}*/
