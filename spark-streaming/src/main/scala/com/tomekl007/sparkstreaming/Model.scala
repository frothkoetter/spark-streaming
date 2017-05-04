package com.tomekl007.sparkstreaming

import java.time.ZonedDateTime

case class PageView(pageViewId: Int, userId: String, url: String, eventTime: ZonedDateTime)