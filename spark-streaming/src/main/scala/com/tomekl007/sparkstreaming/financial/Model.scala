package com.tomekl007.sparkstreaming.financial

import com.tomekl007.WithUserId

case class Payment(userId: String, to: String, amount: BigDecimal) extends WithUserId

case class PaymentValidated(userId: String,
                            to: String,
                            amount: BigDecimal
                           )
  extends WithUserId

object PaymentValidated {
  def fromPayment: (Payment) => PaymentValidated = {
    p => PaymentValidated(p.userId, p.to, p.amount)
  }

}
