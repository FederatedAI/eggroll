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

package com.webank.eggroll.core.util

import java.io.File
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, SQLException, Statement, Types}

import org.apache.commons.lang3.StringUtils

import scala.io.BufferedSource

class JdbcTemplate(dataSource: () => Connection, autoClose: Boolean = true) extends Logging {
  def this(connection: Connection, autoClose: Boolean) = {
    this(() => connection, autoClose)
  }
  def withConnection[T](func: Connection => T): T = {
    val connection = this.dataSource()
    // defaulting transaction level to REPEATABLE_READ
    connection.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE)
    try {
      func(connection)
    } finally {
      if(autoClose) connection.close()
    }
  }

  def withTransaction[T](func: Connection => T): T = withConnection { connection =>
    val oldAutoCommit = connection.getAutoCommit
    connection.setAutoCommit(false)

    try {
      val ret = func(connection)
      connection.commit()
      return ret
    } catch {
      case e: Exception =>
        connection.rollback()
        throw e
    } finally {
      connection.setAutoCommit(oldAutoCommit)
    }
  }

  def withStatement[T](conn: Connection, func: PreparedStatement => T, sql: String, params: Any*): T = {
    var statement: PreparedStatement = null
    try {
      statement = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS)
      params.indices.foreach(i =>
        statement.setObject(i + 1, params(i) match {
          case option: Option[Any] => option.orNull
          case _ => params(i)
        })
      )
      val ret = func(statement)
      statement.close()
      ret
    } catch {
      case e: Exception =>
        val errMsg = s"""error in withStatement. sql="${statement}", params=(${String.join(",", params.map(p => s"'${StringUtils.substring(p.toString, 0, 300)}'"): _*)})"""
        logError(errMsg)
        throw new SQLException(errMsg, e)
    }
  }

  // Prepare statement with sql and params binding
  def withStatement[T](func: PreparedStatement => T, sql: String, params: Any*): T = withConnection {
    withStatement(_, func, sql, params: _*)
  }

  // we need a manager for transactions, or other custom connection config
  def update(conn: Connection, sql: String, params: Any*): Option[Long] = withStatement(
    conn,
    stmt => {
      try{
        stmt.executeUpdate
      } catch {
        case ex: Exception =>
          val errMsg = s"""error in update. sql="${sql}", params=(${String.join(",", params.map(p => s"'${StringUtils.substring(p.toString, 0, 300)}'"): _*)})"""
          logError(errMsg)
          throw new SQLException(errMsg, ex)
      }
      val resultSet = stmt.getGeneratedKeys
      if (resultSet.next &&
         (resultSet.getMetaData.getColumnType(1) == Types.BIGINT ||
          resultSet.getMetaData.getColumnType(1) == Types.INTEGER))
        Some(resultSet.getLong(1))
      else None
    },
    sql, params: _*)

  def update(sql: String, params: Any*): Option[Long] = withConnection(update(_, sql, params: _*))

  def query[T](func: ResultSet => T, sql: String, params: Any*): T = withStatement(
    stmt => {
      val resultSet = stmt.executeQuery
      val ret = func(resultSet)
      resultSet.close()
      ret
    },
    sql,
    params: _*)

  def queryInt(sql: String, params: Any*): Option[Int] = query(
    rs => if (rs.next) Some(rs.getInt(1)) else None, sql, params: _*)

  def queryLong(sql: String, params: Any*): Option[Long] = query(
    rs => if (rs.next) Some(rs.getLong(1)) else None, sql, params: _*)

  def queryOne[T](sql: String, params: Any*): Option[T] = query(
    rs => if (rs.next) Some(rs.getObject(1).asInstanceOf[T]) else None, sql, params: _*)

  def queryTsv(sql: String, params: Any*): String = query({
    rs =>
      val sb = new StringBuilder()
      sb ++= (1 to rs.getMetaData.getColumnCount).map(rs.getMetaData.getColumnName).mkString("\t")
      sb ++= "\n"
      while(rs.next()) {
        sb ++= (1 to rs.getMetaData.getColumnCount).map(rs.getObject).mkString("\t")
        sb ++= "\n"
      }
      if(sb.nonEmpty){
        sb.setLength(sb.length - 1)
      }
      sb.toString()
  }, sql, params: _*)
}

object JdbcTemplate {
  implicit class ResultSetIterator(resultSet: ResultSet) extends Iterable[ResultSet] {
    override def iterator: Iterator[ResultSet] = new Iterator[ResultSet] {
      private var end: Boolean = _
      override def hasNext: Boolean = {

        end = resultSet.next()
        end
      }

      override def next(): ResultSet = resultSet
    }
  }
  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("usage: java  com.webank.eggroll.core.util.JdbcTemplate [jdbc_url] [sql or sql file]")
      return
    }
    val url = args.head
    val sql_arg = args(1)
    val sql = if (new File(sql_arg).exists()) {
      var br:BufferedSource = null
      try  {
        br = scala.io.Source.fromFile(sql_arg, "utf8")
        br.getLines().mkString("")
      } finally {
        br.close()
      }
    } else {
      sql_arg
    }
    val dbc = new JdbcTemplate(DriverManager.getConnection(url), true)
    if (sql.startsWith("select ") || sql.startsWith("show ")){
      println(dbc.queryTsv(sql))
    } else {
      println(dbc.update(sql))
    }

  }
}