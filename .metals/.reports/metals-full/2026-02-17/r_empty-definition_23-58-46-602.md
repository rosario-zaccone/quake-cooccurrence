error id: file://<WORKSPACE>/build.sbt:
file://<WORKSPACE>/build.sbt
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:
	 -Dependencies.libraryDependencies.
	 -Dependencies.libraryDependencies#
	 -Dependencies.libraryDependencies().
	 -libraryDependencies.
	 -libraryDependencies#
	 -libraryDependencies().
	 -scala/Predef.libraryDependencies.
	 -scala/Predef.libraryDependencies#
	 -scala/Predef.libraryDependencies().
offset: 306
uri: file://<WORKSPACE>/build.sbt
text:
```scala
import Dependencies._

ThisBuild / scalaVersion     := "2.13.16"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.example"
ThisBuild / organizationName := "example"

lazy val root = (project in file("."))
  .settings(
    name := "quake-cooccurrence",
    libraryDepend@@encies += munit % Test
  )

// See https://www.scala-sbt.org/1.x/docs/Using-Sonatype.html for instructions on how to publish to Sonatype.

```


#### Short summary: 

empty definition using pc, found symbol in pc: 