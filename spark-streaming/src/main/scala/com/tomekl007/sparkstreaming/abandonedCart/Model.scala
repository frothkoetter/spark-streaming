package com.tomekl007.sparkstreaming.abandonedCart

import com.tomekl007.WithUserId

sealed trait CartEvent extends WithUserId

case class AddToCart(userId: String) extends CartEvent

case class RemoveFromCart(userId: String) extends CartEvent

case class AbandonedCartNotification(userId: String) extends WithUserId