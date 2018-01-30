const aws = require('aws-sdk');
const Promise = require('bluebird')

const readFile = Promise.promisify(require('fs').readFile)

class ServerlessAthenaPlugin {
  constructor(serverless, options) {
    this.serverless = serverless
    this.options = options
    aws.config.update({ region: this.serverless.service.provider.region })
    this.athena = new aws.Athena()

    this.commands = {
      deploy: {
        commands: {
          athenatables: {
            usage: 'Deploy Athena tables',
            lifecycleEvents: [
              'deploy',
            ],
            options: {
              table: {
                usage: 'Table name to deploy',
                required: false,
              },
            },
          }
        }
      },
      remove: {
        commands: {
          athenatables: {
            usage: 'Remove Athena tables',
            lifecycleEvents: [
              'remove',
            ],
            options: {
              table: {
                usage: 'Table name to remove',
                required: false,
              },
            },
          }
        }
      }
    }

    this.hooks = {
      'before:deploy:deploy': this.validateAthenaTablesGlobal.bind(this),
      'after:deploy:deploy': this.createAthenaTablesGlobal.bind(this),
      'after:remove:remove': this.deleteAthenaTablesGlobal.bind(this),
      'deploy:athenatables:deploy': this.deployAthenaTablesDeploy.bind(this),
      'remove:athenatables:remove': this.removeAthenaTablesRemove.bind(this)
    }

    this.shim()
  }

  /**
   * Get all defined Athena tables.
   * Tables should be defined in a custom.athenaTables section in serverless.yml.
   *
   * @returns {*|{}}
   */
  getAthenaTables() {
    return this.serverless.service.custom && this.serverless.service.custom.athenaTables || {}
  }

  /**
   * Resolve when the given Athena query is done
   *
   * @param id
   */
  waitForAthenaQuery(id) {
    return new Promise((resolve, reject) => {
      const isQueryComplete = () =>
        this.athena.getQueryExecution({ QueryExecutionId: id }).promise()
          .then((response) => {
            switch (response.QueryExecution.Status.State) {
              case 'SUCCEEDED':
                return resolve(id)
              case 'RUNNING':
              case 'QUEUED':
                setTimeout(isQueryComplete, 1000)
                break
              case 'FAILED':
              case 'CANCELLED':
              default:
                return reject(`Query status ${response.QueryExecution.Status.State}`)
            }
          })
      isQueryComplete()
    })
  }

  /**
   * Validate a table definition.  Returns validation success.
   *
   * @param tableName
   * @param table
   *
   * @returns {boolean}
   */
  validateAthenaTable(tableName, table) {
    if (!table.DDLFile && !table.DDL) {
      throw new this.serverless.classes.Error(
        `Definition for Athena table ${tableName} must include one of a DDLFile or DDL entry.`)
    }
    if (table.DDLFile && table.DDL) {
      throw new this.serverless.classes.Error(
        `Definition for Athena table ${tableName} must include one of a DDLFile or DDL entry, not both.`)
    }
    if (!table.OutputLocation) {
      throw new this.serverless.classes.Error(
        `Definition for Athena table ${tableName} must include OutputLocation.`)
    }
    if (!table.TableName) {
      throw new this.serverless.classes.Error(
        `Definition for Athena table ${tableName} must include TableName to allow dropping the table on remove.`)
    }
    if (!table.DatabaseName) {
      this.serverless.cli.log(`Warning: Athena table ${tableName} definition does not include DatabaseName; default database will be used`)
    }
    return Promise.resolve()
  }

  /**
   * Validate all defined Athena tables.  Called before global deploy:deploy.
   *
   * @returns {*}
   */
  validateAthenaTablesGlobal() {
    const athenaTables = this.getAthenaTables()
    if (Object.keys(athenaTables).length > 0) {
      this.serverless.cli.log('Validating Athena tables...')
      return Promise.all(Object.entries(athenaTables),
          tableEntry => this.validateAthenaTable(...tableEntry))
    }
  }

  /**
   * Create all defined Athena tables.  Called after global deploy:deploy.
   *
   * @returns {*}
   */
  createAthenaTablesGlobal() {
    const athenaTables = this.getAthenaTables()
    if (Object.keys(athenaTables).length > 0) {
      this.serverless.cli.log('Creating Athena tables...')
      return this.createAthenaTables(athenaTables)
    }
  }

  /**
   * Delete all Athena tables.  Called after global remove:remove.
   *
   * @returns {*}
   */
  deleteAthenaTablesGlobal() {
    const athenaTables = this.getAthenaTables()
    if (Object.keys(athenaTables).length > 0) {
      this.serverless.cli.log('Removing Athena tables...')
      return this.deleteAthenaTables(athenaTables)
    }
  }

  /**
   * Create Athena table(s).  Called with deploy:athenatables:deploy command.
   *
   * @returns {*}
   */
  deployAthenaTablesDeploy() {
    const athenaTables = this.getAthenaTables()

    // If table option is set, create just that table
    if (this.options.table) {
      const table = athenaTables[this.options.table]
      if (table) {
        this.serverless.cli.log(`Creating Athena table ${table}...`)
        return this.createAthenaTable(this.options.table, table)
      }
      return Promise.reject(new Error(`Athena table not found: ${table}`))
    }

    // Table option not set, create all tables
    if (Object.keys(athenaTables).length > 0) {
      this.serverless.cli.log('Creating all Athena tables...')
      return this.createAthenaTables(athenaTables)
    }
    this.serverless.cli.log(
      'No Athena tables found.  Define them in a custom.athenatables section in serverless.yml.')
    return Promise.resolve()
  }

  /**
   * Delete Athena table(s).  Called with remove:athenatables:remove command.
   *
   * @returns {*}
   */
  removeAthenaTablesRemove() {
    const athenaTables = this.getAthenaTables()

    // If table option is set, remove just that table
    if (this.options.table) {
      const table = athenaTables[this.options.table]
      if (table) {
        this.serverless.cli.log(`Removing Athena table ${table}...`)
        return this.deleteAthenaTable(this.options.table, table)
      }
      return Promise.reject(new Error(`Athena table not found: ${table}`))
    }

    // Table option not set, remove all tables
    if (Object.keys(athenaTables).length > 0) {
      this.serverless.cli.log('Removing all Athena tables...')
      return this.deleteAthenaTables(athenaTables)
    }
    this.serverless.cli.log(
      'No Athena tables found.  Define them in a custom.athenatables section in serverless.yml.')
    return Promise.resolve()
  }

  /**
   * Create all the Athena tables
   *
   * @param tables
   */
  createAthenaTables(tables) {
    return Promise.map(Object.entries(tables), tableEntry => this.createAthenaTable(...tableEntry),
      { concurrency: 1 })
      .then(() => {
        this.serverless.cli.log('Athena tables created successfully.')
        return Promise.resolve()
      })
  }

  /**
   * Create one Athena table
   *
   * @param tableName
   * @param table
   */
  createAthenaTable(tableName, table) {
    return this.validateAthenaTable(tableName, table)
      .then(() => this.deleteAthenaTable(tableName, table, true))
      .then(() => {
        if (table.DDLFile) {
          return readFile(table.DDLFile, 'utf8')
        }
        return Promise.resolve(table.DDL)
      })
      .then((ddl) => {
        if (table.DDLSubstitutions) {
          return Promise.resolve(Object.entries(table.DDLSubstitutions).reduce(
            (modifiedDDL, replacement) =>
              modifiedDDL.replace('{' + replacement[0] + '}', replacement[1]), ddl))
        }
        return Promise.resolve(ddl)
      })
      .then((ddl) => {
        const params = {
          QueryString: ddl,
          ResultConfiguration: { OutputLocation: table.OutputLocation }
        }
        if (table.DatabaseName) {
          params.QueryExecutionContext = { Database: table.DatabaseName }
        }
        this.serverless.cli.log(`Creating Athena table ${tableName}...`)
        return this.athena.startQueryExecution(params).promise()
      })
      .then(executionResponse => this.waitForAthenaQuery(executionResponse.QueryExecutionId))
  }

  /**
   * Delete all the Athena tables passed in
   *
   * @param tables
   */
  deleteAthenaTables(tables) {
    return Promise.map(Object.entries(tables), tableEntry => this.deleteAthenaTable(...tableEntry),
      { concurrency: 1 })
      .then(() => {
        this.serverless.cli.log('Athena tables deleted successfully.')
        return Promise.resolve()
      })
  }

  /**
   * Delete a single Athena table
   *
   * @param tableName
   * @param table
   * @param ifExists
   */
  deleteAthenaTable(tableName, table, ifExists = false) {
    const params = {
      QueryString: `DROP TABLE ${ifExists ? 'IF EXISTS' : ''} ${table.TableName}`,
      ResultConfiguration: { OutputLocation: table.OutputLocation }
    }
    if (table.DatabaseName) {
      params.QueryExecutionContext = { Database: table.DatabaseName }
    }
    this.serverless.cli.log(`Removing Athena table ${tableName} (${table.TableName})...`)
    return this.athena.startQueryExecution(params).promise()
      .then(executionResponse => this.waitForAthenaQuery(executionResponse.QueryExecutionId))
  }

  shim() {
    if (!Object.entries)
      Object.entries = function(obj) {
        const ownProps = Object.keys(obj)
        let i = ownProps.length
        const resArray = new Array(i) // preallocate the Array
        while (i--)
          resArray[i] = [ownProps[i], obj[ownProps[i]]]

        return resArray
      }
  }
}

module.exports = ServerlessAthenaPlugin
