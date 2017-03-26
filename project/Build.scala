import com.typesafe.sbt.SbtScalariform
import de.heikoseeberger.sbtheader.HeaderPlugin
import de.heikoseeberger.sbtheader.license.Apache2_0
import sbt._

import scalariform.formatter.preferences.{AlignSingleLineCaseStatements, DoubleIndentClassDeclaration, SpacesAroundMultiImports}

object Build extends AutoPlugin {

  override def requires = SbtScalariform && HeaderPlugin

  override def trigger = allRequirements

  override def projectSettings: Seq[Def.Setting[_]] = Vector(

    SbtScalariform.autoImport.scalariformPreferences := SbtScalariform.autoImport.scalariformPreferences.value
      .setPreference(AlignSingleLineCaseStatements, true)
      .setPreference(SpacesAroundMultiImports, false)
      .setPreference(AlignSingleLineCaseStatements.MaxArrowIndent, 100)
      .setPreference(DoubleIndentClassDeclaration, true),

    HeaderPlugin.autoImport.headers := Map(
      "scala" -> Apache2_0("2017", "Radek Gruchalski"),
      "java" -> Apache2_0("2017", "Radek Gruchalski")
    )

  )

}