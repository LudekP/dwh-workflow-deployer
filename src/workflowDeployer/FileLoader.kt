package workflowDeployer

import java.io.File

open class FileLoader {

    open fun loadFromFolder(folderPath: String) : Map<String, File> {
        val fileContents = HashMap<String, File>()
        File(folderPath).walk()
            .filter{ file -> file.isFile}
            .filter{ file -> file.extension == "bpmn" }
            .forEach {
                fileContents[it.name] = it
            }
        return  fileContents
    }

}