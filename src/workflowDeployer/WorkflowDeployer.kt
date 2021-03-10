package workflowDeployer

import java.io.FileInputStream
import java.util.logging.LogManager
import java.util.logging.Logger
import kotlin.system.exitProcess

class WorkflowDeployer(args: Array<String>) {

    private lateinit var fileLoader : FileLoader
    private lateinit var databaseConnector: DatabaseConnector
    private lateinit var userName: String
    private lateinit var password: String
    private lateinit var connectionString: String
    private lateinit var directory: String
    private var deploy: String = "Y"

    init {
        // Parse parameters from arguments
        for (i in args.indices) {
            with(args[i]) {
                when {
                    contains("--userName=")         -> userName         = substring(indexOf("=") + 1)
                    contains("--password=")         -> password         = substring(indexOf("=") + 1)
                    contains("--connectionString=") -> connectionString = "jdbc:oracle:thin:@" + substring(indexOf("=") + 1)
                    contains("--directory=")        -> directory        = substring(indexOf("=") + 1)
                    contains("--deploy=")           -> deploy           = substring(indexOf("=") + 1)
                }
            }
        }
        LogManager.getLogManager().readConfiguration(FileInputStream("logging.properties"))
    }

    fun runWorkflowDeployer() {

        // Load workflow processes from folder
        fileLoader = FileLoader()
        // Load workflow processes from folder
        logger.info("Going to load processes from directory $directory")
        val loadedFiles = fileLoader.loadFromFolder(directory)
        logger.info("Loaded processes count: ${loadedFiles.size}.")

        // Validate and deploy processes
        if(loadedFiles.isNotEmpty()) {

            // Create db connection
            databaseConnector = DatabaseConnector(userName, password, connectionString)
            databaseConnector.createConnection()

            // Upload workflow
            databaseConnector.uploadWorkflow(loadedFiles)

            // Validate workflow
            databaseConnector.validateWorkflow()

            // Deploy workflow
            if (deploy == "Y") {
                databaseConnector.deployWorkflow()
            } else
                logger.info("Skipping deployment of workflow based on input parameter.")
            // Close db connection
            databaseConnector.closeConnection()

        } else logger.info("Skipping deployment of workflow because no processes found in input folder.")
        exitProcess(0)

    }

    companion object {
        var logger: Logger = Logger.getLogger(WorkflowDeployer::class.java.toString())
        val mandatoryOptions = mutableListOf("userName", "password", "connectionString", "directory")
        const val usageOutput = "\nUsage example: java -jar workflow-deployer-VERSION.jar --userName=<databaseUserName> --password=<Password> --connectionString=//DBCN00C8HD.CN.INFRA:1521/CN00C8HD.CN.INFRA --folder=C:/some/path"
    }

}

fun validateArguments(args: Array<String>): Boolean {
    return if(!ArgsUtils.containsMandatoryOptions(args)) {
        Logger.getLogger("StartupValidationLogger").warning("ERROR running workflow deployer. Options ${WorkflowDeployer.mandatoryOptions} are mandatory! " +
                WorkflowDeployer.usageOutput)
        false
    } else {
        true
    }
}

object ArgsUtils {
    fun containsMandatoryOptions(args : Array<String>): Boolean {
        var allOptionsPresent = true
        WorkflowDeployer.mandatoryOptions.forEach { if(!containsOption(args, it)) allOptionsPresent = false }
        return allOptionsPresent
    }

    private fun containsOption(args : Array<String>, option: String): Boolean {
        return args.any { item -> item.startsWith("--$option") }
    }
}

fun main(args: Array<String>) {
    if(validateArguments(args)) {
        val wfDeployer = WorkflowDeployer(args)
        wfDeployer.runWorkflowDeployer()
        }
    }
