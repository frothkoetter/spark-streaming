package com.tomekl007.sparkstreaming.ordering

import java.time.ZonedDateTime

import com.tomekl007.sparkstreaming.PageView

class StreamOrderVerification() {
  private var lastEventsActionDatePerUserId: Map[String, ZonedDateTime] = Map()
  //on production it should be a cache with an expiring time to prevent OutOfMemory

  def isInOrder(newEvent: PageView): Boolean = {
    val userId = newEvent.userId
    val isInOrder = lastEventsActionDatePerUserId.get(userId)
      .forall(lastActionDate => inOrder(newEvent, lastActionDate))
    println(s"inOrder: $newEvent, result: $isInOrder, ${ZonedDateTime.now()}")
    lastEventsActionDatePerUserId += (userId -> newEvent.eventTime)

    isInOrder
  }

  private def inOrder(newEvent: PageView, lastEventActionDate: ZonedDateTime) =
    newEvent.eventTime.isAfter(lastEventActionDate)
}