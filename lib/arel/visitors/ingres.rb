module Arel
  module Visitors
    class Ingres < Arel::Visitors::ToSql
      private
      def visit_Arel_Nodes_SelectStatement o
        [
          "SELECT",
          (visit(o.limit) if o.limit),
          o.cores.map { |x| visit_Arel_Nodes_SelectCore x }.join,
          ("ORDER BY #{o.orders.map { |x| visit x}.join(', ')}" unless o.orders.empty?),
          (visit(o.offset) if o.offset),
        ].compact.join ' '
      end

      def visit_Arel_Nodes_SelectCore o
        [
          "#{o.projections.map { |x| visit x }.join ', '}",
          ("FROM #{visit(o.source)}" if o.source && !o.source.empty?),
          ("WHERE #{o.wheres.map { |x| visit x }.join ' AND ' }" unless o.wheres.empty?),
          ("GROUP BY #{o.groups.map { |x| visit x }.join ', ' }" unless o.groups.empty?),
          (visit(o.having) if o.having),
        ].compact.join ' '
      end

      def visit_Arel_Nodes_Limit o
        "FIRST #{visit o.expr}"
      end
    end
  end
end
