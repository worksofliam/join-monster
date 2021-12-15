const {
  keysetPagingSelect,
  offsetPagingSelect,
  interpretForOffsetPaging,
  interpretForKeysetPaging,
  generateCastExpressionFromValueType
} = require('../shared')

const dialect = (module.exports = {
  name: 'db2',

  quote(str) {
    return `"${str}"`
  },

  compositeKey(parent, keys) {
    keys = keys.map(key => `${parent.toUpperCase()}.${key.toUpperCase()}`)
    return `NULLIF(CONCAT(${keys.join(', ')}), '')`
  },

  handleJoinedOneToManyPaginated: async function(
    parent,
    node,
    context,
    tables,
    joinCondition
  ) {
    const pagingWhereConditions = [
      await node.sqlJoin(
        `${parent.as.toUpperCase()}`,
        `${node.as.toUpperCase()}`,
        node.args || {},
        context,
        node
      )
    ]
    if (node.where) {
      pagingWhereConditions.push(
        await node.where(`${node.as.toUpperCase()}`, node.args || {}, context, node)
      )
    }

    // which type of pagination are they using?
    if (node.sortKey) {
      const {
        limit,
        order,
        whereCondition: whereAddendum
      } = interpretForKeysetPaging(node, dialect)
      pagingWhereConditions.push(whereAddendum)
      tables.push(
        keysetPagingSelect(
          node.name,
          pagingWhereConditions,
          order,
          limit,
          node.as,
          { joinCondition, joinType: 'LEFT' }
        )
      )
    } else if (node.orderBy) {
      const { limit, offset, order } = interpretForOffsetPaging(node, dialect)
      tables.push(
        offsetPagingSelect(
          node.name,
          pagingWhereConditions,
          order,
          limit,
          offset,
          node.as,
          {
            joinCondition,
            joinType: 'LEFT'
          }
        )
      )
    }
  },

  handleBatchedManyToManyPaginated: async function(
    parent,
    node,
    context,
    tables,
    batchScope,
    joinCondition
  ) {
    const thisKeyOperand = generateCastExpressionFromValueType(
      `${node.junction.as.toUpperCase()}.${node.junction.sqlBatch.thisKey.name.toUpperCase()}`,
      batchScope[0]
    )
    const pagingWhereConditions = [
      `${thisKeyOperand} = temp.${node.junction.sqlBatch.parentKey.name.toUpperCase()}`
    ]
    if (node.junction.where) {
      pagingWhereConditions.push(
        await node.junction.where(
          `${node.junction.as.toUpperCase()}`,
          node.args || {},
          context,
          node
        )
      )
    }
    if (node.where) {
      pagingWhereConditions.push(
        await node.where(`${node.as.toUpperCase()}`, node.args || {}, context, node)
      )
    }

    const tempTable = `FROM (VALUES ${batchScope.map(
      val => `(${val})`
    )}) temp(${node.junction.sqlBatch.parentKey.name}.toUpperCase())`
    tables.push(tempTable)
    const lateralJoinCondition = `${thisKeyOperand} = temp.${node.junction.sqlBatch.parentKey.name.toUpperCase()}`

    const lateralJoinOptions = {
      joinCondition: lateralJoinCondition,
      joinType: 'LEFT'
    }
    if (node.where || node.orderBy) {
      lateralJoinOptions.extraJoin = {
        name: node.name,
        as: node.as,
        condition: joinCondition
      }
    }
    if (node.sortKey || node.junction.sortKey) {
      const {
        limit,
        order,
        whereCondition: whereAddendum
      } = interpretForKeysetPaging(node, dialect)
      pagingWhereConditions.push(whereAddendum)
      tables.push(
        keysetPagingSelect(
          node.junction.sqlTable,
          pagingWhereConditions,
          order,
          limit,
          node.junction.as,
          lateralJoinOptions
        )
      )
    } else if (node.orderBy || node.junction.orderBy) {
      const { limit, offset, order } = interpretForOffsetPaging(node, dialect)
      tables.push(
        offsetPagingSelect(
          node.junction.sqlTable,
          pagingWhereConditions,
          order,
          limit,
          offset,
          node.junction.as,
          lateralJoinOptions
        )
      )
    }
    tables.push(`LEFT JOIN ${node.name.toUpperCase()} AS ${node.as.toUpperCase()} ON ${joinCondition}`)
  },

  handleJoinedManyToManyPaginated: async function(
    parent,
    node,
    context,
    tables,
    joinCondition1,
    joinCondition2
  ) {
    const pagingWhereConditions = [
      await node.junction.sqlJoins[0](
        `${parent.as.toUpperCase()}`,
        `${node.junction.as.toUpperCase()}`,
        node.args || {},
        context,
        node
      )
    ]
    if (node.junction.where) {
      pagingWhereConditions.push(
        await node.junction.where(
          `${node.junction.as.toUpperCase()}`,
          node.args || {},
          context,
          node
        )
      )
    }
    if (node.where) {
      pagingWhereConditions.push(
        await node.where(`${node.as.toUpperCase()}`, node.args || {}, context, node)
      )
    }

    const lateralJoinOptions = {
      joinCondition: joinCondition1,
      joinType: 'LEFT'
    }
    if (node.where || node.orderBy) {
      lateralJoinOptions.extraJoin = {
        name: node.name,
        as: node.as,
        condition: joinCondition2
      }
    }
    if (node.sortKey || node.junction.sortKey) {
      const {
        limit,
        order,
        whereCondition: whereAddendum
      } = interpretForKeysetPaging(node, dialect)
      pagingWhereConditions.push(whereAddendum)
      tables.push(
        keysetPagingSelect(
          node.junction.sqlTable,
          pagingWhereConditions,
          order,
          limit,
          node.junction.as,
          lateralJoinOptions
        )
      )
    } else if (node.orderBy || node.junction.orderBy) {
      const { limit, offset, order } = interpretForOffsetPaging(node, dialect)
      tables.push(
        offsetPagingSelect(
          node.junction.sqlTable,
          pagingWhereConditions,
          order,
          limit,
          offset,
          node.junction.as,
          lateralJoinOptions
        )
      )
    }
  },

  handlePaginationAtRoot: async function(parent, node, context, tables) {
    const pagingWhereConditions = []
    if (node.sortKey) {
      const {
        limit,
        order,
        whereCondition: whereAddendum
      } = interpretForKeysetPaging(node, dialect)
      pagingWhereConditions.push(whereAddendum)
      if (node.where) {
        pagingWhereConditions.push(
          await node.where(`${node.as.toUpperCase()}`, node.args || {}, context, node)
        )
      }
      tables.push(
        keysetPagingSelect(
          node.name,
          pagingWhereConditions,
          order,
          limit,
          node.as
        )
      )
    } else if (node.orderBy) {
      const { limit, offset, order } = interpretForOffsetPaging(node, dialect)
      if (node.where) {
        pagingWhereConditions.push(
          await node.where(`${node.as.toUpperCase()}`, node.args || {}, context, node)
        )
      }
      tables.push(
        offsetPagingSelect(
          node.name,
          pagingWhereConditions,
          order,
          limit,
          offset,
          node.as
        )
      )
    }
  },

  handleBatchedOneToManyPaginated: async function(
    parent,
    node,
    context,
    tables,
    batchScope
  ) {
    const thisKeyOperand = generateCastExpressionFromValueType(
      `${node.as.toUpperCase()}.${node.sqlBatch.thisKey.name.toUpperCase()}`,
      batchScope[0]
    )
    const pagingWhereConditions = [
      `${thisKeyOperand} = temp.${node.sqlBatch.parentKey.name.toUpperCase()}`
    ]
    if (node.where) {
      pagingWhereConditions.push(
        await node.where(`${node.as.toUpperCase()}`, node.args || {}, context, node)
      )
    }
    const tempTable = `FROM (VALUES ${batchScope.map(
      val => `(${val})`
    )}) temp(${node.sqlBatch.parentKey.name.toUpperCase()})`
    tables.push(tempTable)
    const lateralJoinCondition = `${thisKeyOperand} = temp.${node.sqlBatch.parentKey.name.toUpperCase()}`
    if (node.sortKey) {
      const {
        limit,
        order,
        whereCondition: whereAddendum
      } = interpretForKeysetPaging(node, dialect)
      pagingWhereConditions.push(whereAddendum)
      tables.push(
        keysetPagingSelect(
          node.name,
          pagingWhereConditions,
          order,
          limit,
          node.as,
          { joinCondition: lateralJoinCondition }
        )
      )
    } else if (node.orderBy) {
      const { limit, offset, order } = interpretForOffsetPaging(node, dialect)
      tables.push(
        offsetPagingSelect(
          node.name,
          pagingWhereConditions,
          order,
          limit,
          offset,
          node.as,
          {
            joinCondition: lateralJoinCondition
          }
        )
      )
    }
  }
})
