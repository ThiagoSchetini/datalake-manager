package br.com.bvs.datalake.helper

import org.apache.hadoop.fs.Path

object PathHelper {

  def buildOngoingPath(smPath: Path, ongoingDirName: String) = {

    /* check if it's already an ongoing dir and returns the same if true */
    smPath.getParent.getName == ongoingDirName match {
      case true => smPath
      case false => new Path(s"${smPath.getParent}/$ongoingDirName/${smPath.getName}")
    }
  }

  def buildDonePath(path: Path, doneDirName: String): Path = {
    new Path(s"${path.getParent.getParent}/$doneDirName/${path.getName}")
  }

  def buildFailPath(path: Path, failDirName: String): Path = {
    new Path(s"${path.getParent.getParent}/$failDirName/${path.getName}")
  }

  def buildErrorFilePath(path: Path, sufix: String, failDirName: String): Path = {
    val name = path.getName.replace(s".$sufix", "")
    val errorName = s"$name.error"
    new Path(s"${path.getParent.getParent}/$failDirName/$errorName")
  }

}
