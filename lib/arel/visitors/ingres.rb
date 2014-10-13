module Arel
  module Visitors
    class Ingres < Arel::Visitors::ToSql
      private

      def visit_Arel_Nodes_SelectStatement o, collector
        collector << "SELECT "

        if o.limit
          collector << "FIRST #{o.limit} "
        end

        visit o.relation, collector

        unless o.wheres.empty?
          collector << " WHERE "
          collector = inject_join o.wheres, collector, ' AND '
        end

        unless o.orders.empty?
          collector << " ORDER BY "
          collector = inject_join o.orders, collector, ', '
        end

        collector
      end
    end
  end
end
