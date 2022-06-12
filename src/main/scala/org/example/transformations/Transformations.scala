package org.example.transformations

import org.example.`case`.{Actor, Name}

case object Transformations {
  def higherThan(names: Name, h: Integer): Boolean = {
    if (names.height == null) return false
    names.height > h
  }

  def isCategory(actors: Actor, job: String): Boolean = {
    if (job == null) return false
    actors.job == job
  }
}
