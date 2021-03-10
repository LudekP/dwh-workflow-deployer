package workflowDeployer

import java.io.File
import java.io.StringReader
import java.net.InetAddress
import java.net.UnknownHostException
import java.nio.file.Files
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import java.sql.Types
import java.util.*
import java.util.logging.Logger
import kotlin.system.exitProcess


open class DatabaseConnector(private val userName: String,
                             private val password: String,
                             private val connectionString: String) {

    private var idDeployment : Int = 0
    private lateinit var connection : Connection
    private var logger: Logger = Logger.getLogger(this.javaClass.toString())

    open fun getLocalHostName(): String {
        return try {
            val address = InetAddress.getLocalHost()
            address.hostName
        } catch (ex: UnknownHostException) {
            "Unknown"
        }
    }

    open fun createConnection() {
        try {
            // Create connection
            val props = Properties()
            props["v\$session.machine"] = InetAddress.getLocalHost().canonicalHostName
            props["v\$session.program"] = "Workflow Deployer"
            this.connection = DriverManager.getConnection(connectionString, userName, password)
            // Get database name
            val statement = this.connection.prepareCall("{? = call ora_database_name}")
            statement.registerOutParameter(1, Types.VARCHAR)
            statement.execute()
            // Set retrieved values
            val databaseName = statement.getString(1)
            // Log message
            logger.info("Connected to the database $databaseName using URL ${this.connection.metaData.url}.")
        } catch (e: SQLException) {
            // Log error message and exit application
            logger.severe("Connection Failed! Check output:")
            e.printStackTrace()
            exitProcess(-1)
        }
    }

    open fun closeConnection() {
        try {
            // Close connection
            this.connection.close()
        } catch (e: SQLException) {
            // Log error message
            logger.severe("Close of database connection Failed! Check output:")
            e.printStackTrace()
            return
        }
    }

    open fun uploadWorkflow(process : Map<String, File> ){

        try {
            // Purge workflow files before uploading
            logger.info("Purge workflow files before uploading to database.")
            var callStatement = this.connection.prepareCall("{call owner_wfm.lib_etl_wf_deployer_api.purge_workflow_file(p_code_result => ?, p_text_message => ?)}")
            callStatement.registerOutParameter(1, Types.VARCHAR)
            callStatement.registerOutParameter(2, Types.CLOB)
            callStatement.execute()

            if (callStatement.getString(1) == "ERROR") {
                logger.severe("Purge of workflow files before upload failed on ${callStatement.getString(2)}, processes won't be deployed!")
                closeConnection()
                exitProcess(-1)
            }

            // Get deployment id
            logger.info("Get deployment id.")
            callStatement = this.connection.prepareCall("{? = call owner_wfm.lib_etl_wf_deployer_api.get_id_deployment}")
            callStatement.registerOutParameter(1, Types.INTEGER)
            callStatement.execute()
            // Set id deployment
            this.idDeployment = callStatement.getInt(1)

            if (callStatement.getString(1) == "ERROR") {
                logger.severe("Purge of workflow files before upload failed on ${callStatement.getString(2)}, workflow won't be deployed!")
                closeConnection()
                exitProcess(-1)
            }

            // Upload Workflow
            logger.info("Uploading workflow files to database.")
            val sqlStatement = this.connection.prepareStatement("INSERT /*+ append */ INTO owner_wfm.v_etl_wf_file2deployment(id_deployment, name_workflow_file, text_workflow, dtime_inserted, user_inserted) VALUES (?, ?, ?, SYSDATE, ?)")
            process.forEach{
                sqlStatement.setInt(1, this.idDeployment)
                sqlStatement.setString(2, it.key)
                sqlStatement.setCharacterStream(3, StringReader(String(Files.readAllBytes(it.value.toPath()))), String(Files.readAllBytes(it.value.toPath())).length)
                sqlStatement.setString(4, this.userName)
                sqlStatement.addBatch()
            }
            sqlStatement.executeBatch()
            sqlStatement.close()

            // Convert workflow files to XML type
            logger.info("Convert workflow files to XML type.")
            callStatement = this.connection.prepareCall("{call owner_wfm.lib_etl_wf_deployer_api.convert_workflow_file2xmltype(p_id_deployment => ?, p_code_result => ?, p_text_message => ?)}")
            callStatement.setInt(1, this.idDeployment)
            callStatement.registerOutParameter(2, Types.VARCHAR)
            callStatement.registerOutParameter(3, Types.CLOB)
            callStatement.execute()

            if (callStatement.getString(2) == "ERROR") {
                logger.severe("Conversion of workflow files failed on ${callStatement.getString(3)}, workflow won't be deployed!")
                closeConnection()
                exitProcess(-1)
            }

        } catch (e: SQLException) {
            // Log error message and exit application
            logger.severe("Unable to upload workflow files! Check output:")
            e.printStackTrace()
            closeConnection()
            exitProcess(-1)
        }
    }

    open fun validateWorkflow() {

        try {
            // Validate workflow files
            logger.info("Validate workflow files.")
            val callStatement = this.connection.prepareCall("{call owner_wfm.lib_etl_wf_deployer_api.validate_workflow(p_code_result => ?, p_text_message => ?)}")
            callStatement.registerOutParameter(1, Types.VARCHAR)
            callStatement.registerOutParameter(2, Types.CLOB)
            callStatement.execute()

            when (callStatement.getString(1)) {
                "COMPLETE" ->
                    logger.info("Workflow files are valid.")
                "WARNING"  -> {
                    logger.warning("Workflow files are valid, but do not meet all the rules. You should adjust the workflow! Following exceptions were found: \n"+ callStatement.getString(2))
                }
                "ERROR"    -> {
                    logger.severe("Workflow files are invalid, workflow won't be deployed! Following exceptions were found: ${callStatement.getString(2)}")
                    closeConnection()
                    exitProcess(-1)
                }
                else       -> {
                    logger.severe("\"Unsupported result code, processes won't be deployed! Result: ${callStatement.getString(1)}")
                    closeConnection()
                    exitProcess(-1)
                }
            }
        } catch (e: SQLException) {
            // Log error message and exit application
            logger.severe("Unable to validate Workflow files! Check output:")
            e.printStackTrace()
            closeConnection()
            exitProcess(-1)
        }
    }

    open fun deployWorkflow(){

        try {
            // Deploy workflow files
            logger.info("Deploy workflow files to repository.")
            val callStatement = this.connection.prepareCall("{call owner_wfm.lib_etl_wf_deployer_api.deploy_workflow(p_id_deployment => ?, p_name_deployment => ?, p_code_result => ?, p_text_message => ?)}")
            callStatement.setInt(1, this.idDeployment)
            callStatement.setString(2, "workflow-deployer (executed by ${System.getProperty("user.name")} from machine ${getLocalHostName()})")
            callStatement.registerOutParameter(3, Types.VARCHAR)
            callStatement.registerOutParameter(4, Types.CLOB)
            callStatement.execute()

            if (callStatement.getString(3) == "ERROR") {
                logger.severe("Deployment of workflow files failed on ${callStatement.getString(4)}, workflow won't be deployed!")
                closeConnection()
                exitProcess(-1)
            }

        } catch (e: SQLException) {
            // Log error message and exit application
            logger.severe("Unable to upload workflow files! Check output:")
            e.printStackTrace()
            closeConnection()
            exitProcess(-1)
        }
    }
}