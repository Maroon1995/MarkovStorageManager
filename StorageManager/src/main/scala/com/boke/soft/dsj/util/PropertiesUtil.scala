package com.boke.soft.dsj.util

import java.io.InputStreamReader
import java.nio.charset.StandardCharsets
import java.util.Properties

object PropertiesUtil {

  def load(propertiesName:String): Properties ={
    // new一个Properties对象
    val prop = new Properties()
    // 配置文件路径
    val path = new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName),
      StandardCharsets.UTF_8)
    // 加载配置文件
    prop.load(path)
    prop
  }
}
