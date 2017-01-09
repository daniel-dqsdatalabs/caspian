import com.thoughtworks.datacommons.prepbuddy.types.CSV
import org.apache.spark.rdd.RDD

class TableMerger(primaryTable: RDD[String], otherTable: RDD[String]) {
    def merge(primaryKeyIndex: Int, foreignKeyIndex: Int): RDD[String] = {
        val other = tableByIndex(otherTable, primaryKeyIndex)
        val primary = tableByIndex(primaryTable, foreignKeyIndex)
        val transformedTable = primary.join(other)
        transformedTable.map {
            case (key, (pTable, oTable)) => CSV.appendDelimiter(pTable) + oTable
        }
    }

    private def tableByIndex(table: RDD[String], columnIndex: Int): RDD[(String, String)] = {
        table.map((record) => {
            val parse = CSV.parse(record)
            val range = 0 until parse.length
            val selection = range.toBuffer.filter(x => x != columnIndex).toList
            val filtered = parse.select(selection)
            (parse.select(columnIndex), CSV.join(filtered))
        })
    }
}
