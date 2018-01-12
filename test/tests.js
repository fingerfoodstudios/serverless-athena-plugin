'use strict'
require('dotenv').load();

/**
 * Test script for serverless-athena-plugin
 *
 * You should set AWS_DEFAULT_PROFILE and AWS_REGION before running this.
 */

const chai = require('chai')
const chaiAsPromised = require('chai-as-promised')
const AWS = require('aws-sdk')
const childProcess = require('child_process')
const path = require('path')
const chalk = require('chalk')

chai.use(chaiAsPromised)
const assert = chai.assert

const CLOUDFORMATION_STACK = 'serverless-athena-plugin-test-plugintest'
const ATHENA_QUERY_OUTPUT_LOCATION = `s3://${process.env.ATHENA_QUERY_OUTPUT_BUCKET_NAME}/serverless-athena-plugin-test-test1/`
const ATHENA_TABLE_1_NAME = 'athena_plugintest_1'
const ATHENA_TABLE_2_NAME = 'athena_plugintest_2'
const TEST_BUCKET_NAME = 'serverless-athena-plugin-test-test1'
const TEST_FOLDER_KEY = 'stage=plugintest_$folder$'

const cloudformation = new AWS.CloudFormation()
const SLS = path.join(__dirname, '/node_modules', '.bin', 'sls')
const athena = new AWS.Athena()
const s3 = new AWS.S3()

const sls = (args) => {
  console.log('   ', chalk.gray.dim('$'), chalk.gray.dim('sls ' + args.join(' ')))
  const dir = path.join(__dirname, 'service')
  return new Promise((resolve, reject) => {
    const child = childProcess.execFile(SLS, args, {
      cwd: dir,
    }, (err, stdout, stderr) => {
      if (err) return reject(err)
      resolve(stdout)
    })
    child.stdout.on('data', data => {
      process.stdout.write(chalk.gray(data))
    })
    child.stderr.on('data', data => {
      process.stderr.write(chalk.red(data))
    })
  })
}

const describeStack = stackName =>
  cloudformation.describeStacks({
    StackName: stackName,
  }).promise()
    .then(response => {
      return response.Stacks && response.Stacks[0]
    })
    .then(null, err => {
      if (err.message && err.message.match(/does not exist$/)) {
        // Stack doesn't exist yet
        return null
      } else {
        // Some other error, let it throw
        return Promise.reject(err)
      }
    })

const describeAllStacks = () => Promise.all([
    describeStack(CLOUDFORMATION_STACK)
  ])

const listAthenaTables = () =>
  athena.startQueryExecution({
    QueryString: 'SHOW TABLES',
    ResultConfiguration: { OutputLocation: ATHENA_QUERY_OUTPUT_LOCATION },
    QueryExecutionContext: { Database: process.env.ATHENA_DB_NAME }
  }).promise()
    .then(executionResponse => waitForAthenaQuery(executionResponse.QueryExecutionId))
    .then(id => athena.getQueryResults({ QueryExecutionId: id }).promise())
    .then(results => results.ResultSet.Rows.map(r => r.Data[0].VarCharValue))

const waitForAthenaQuery = id =>
  new Promise((resolve, reject) => {
    const isQueryComplete = () =>
      athena.getQueryExecution({ QueryExecutionId: id }).promise()
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

const deleteStack = stackName =>
  Promise.resolve()
    .then(() => {
      return describeStack(stackName)
    })
    .then(response => {
      if (response) {
        console.log('   ', chalk.yellow.dim('Cleaning up stack ' + stackName))
        return cloudformation.deleteStack({
          StackName: stackName,
        })
          .promise()
      }
    })
    .then(response => {
      if (response) {
        return cloudformation.waitFor('stackDeleteComplete', {
          StackName: stackName,
        })
          .promise()
      }
    })

const deleteFolderInS3Bucket = () =>
  s3.deleteObject({ Bucket: TEST_BUCKET_NAME, Key: TEST_FOLDER_KEY }).promise()
    .then(() => Promise.resolve())
    .catch(() => Promise.resolve())

const deleteAllStacks = () =>
  deleteFolderInS3Bucket()
    .then(() => Promise.all([
      deleteStack(CLOUDFORMATION_STACK)
    ]))

describe('Deploy services', () => {
  // after(deleteAllStacks)

  // it('print out environment values', () =>
  //   Promise.resolve()
  //     .then(() => {
  //       console.log(process.env)
  //       return Promise.resolve()
  //     })
  // )

  it('stack deployed on sls deploy creates Athena tables', () =>
    Promise.resolve()
      .then(() => sls(['deploy', '--force', '-v']))
      .then(() => describeAllStacks())
      .then(responses => {
        assert.isOk(responses[0], 'serverless stack')
        assert.equal(responses[0].StackStatus, 'CREATE_COMPLETE', 'serverless stack')
        assert.equal(responses[0].Tags.filter(tag => tag.Key === 'Owner')[0].Value, 'owner@example.org', 'serverless stack custom tag')
      })
      .then(listAthenaTables)
      .then((athenaTables) => {
        assert.lengthOf(athenaTables, 2)
        assert.sameMembers(athenaTables, [ATHENA_TABLE_1_NAME, ATHENA_TABLE_2_NAME])
      })
  )

  it('stack undeployed using sls remove also removes Athena tables', () =>
    Promise.resolve()
      .then(deleteFolderInS3Bucket)
      .then(() => sls(['remove', '-v']))
      .then(listAthenaTables)
      .then((athenaTables) => {
        assert.lengthOf(athenaTables, 0)
      })
  )
})