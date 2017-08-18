package org.jpahelper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.persistence.EntityManager;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;

import org.jpahelper.JpaCriteriaHelper.ComparatorOperator;

/**
 * Classe utilitária para facilitação das chamadas mais simples à JPA utilizando CriteriaBuilder.
 * @author mauricio.guzinski
 *
 */
public class JpaCriteriaHelper<T> {

    public enum ComparatorOperator { EQUAL, NOT_EQUAL, LIKE, LIKE_IGNORE_CASE, BETWEEN, GREATER_THAN, LESS_THAN, IN };
    public enum LogicalOperator { AND, OR };
    public enum OrderDirection { ASC, DESC };

    private static final Integer DEFAULT_PAGE_SIZE = 50;

    private EntityManager em;

    private CriteriaBuilder criteriaBuilder;

    private List<WhereEntry> wheres = new ArrayList<>();

    private List<OrderEntry> orders = new ArrayList<>();

    private Integer pageSize = DEFAULT_PAGE_SIZE;

    private Integer pageNumber;

    private Class<T> entityClass;

    /**
     * Define nome entradas da cláusula WHERE
     *
     */
    private class WhereEntry {

        private String fieldName;

        private ComparatorOperator comparatorOperator;

        private Object valueIni;

        private Object valueEnd;

        private LogicalOperator logicalOperator;

        public WhereEntry(String fieldName
                ,ComparatorOperator comparatorOperator
                ,Object valueIni
                ,Object valueEnd
                ,LogicalOperator logicalOperator) {
            this.fieldName          = fieldName;
            this.comparatorOperator = comparatorOperator;
            this.valueIni           = valueIni;
            this.valueEnd           = valueEnd;
            this.logicalOperator    = logicalOperator;
        }
    }

    /**
     * Define nome do campo a ser ordenado, assim como a direção em que deve ser ordenado
     *
     */
    private class OrderEntry {

        private String fieldName;

        private OrderDirection order;

        public OrderEntry(String fieldName, OrderDirection order) {
            this.fieldName = fieldName;
            this.order = order;
        }
    }

    private JpaCriteriaHelper( EntityManager em, Class<T> entityClass ) {
        this.em               = em;
        this.entityClass      = entityClass;
        this.criteriaBuilder  = em.getCriteriaBuilder();
    }

    /**
     * Cria o objeto de consulta para executar a query
     * @param em EntityManager
     * @param entityClazz Classe de destino
     * @return objeto de consulta
     */
    public static <X> JpaCriteriaHelper<X> select( EntityManager em, Class<X> entityClazz ) {
        return new JpaCriteriaHelper<>( em, entityClazz );
    }

    /**
     * Inclui uma cláusula WHERE com operador {@link ComparatorOperator.EQUAL} implícito
     * @param fieldName fieldName Nome da propriedade
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> where( String fieldName, Object value ) {
        return where(fieldName, ComparatorOperator.EQUAL, value);
    }

    /**
     * Inclui uma cláusula WHERE
     * @param fieldName Nome da propriedade
     * @param comparator Comparador <b>(Para {@link ComparatorOperator.GREATER_THAN} e {@link ComparatorOperator.GREATER_THAN}
     * é necessário que valor complemente {@link Comparable})</b>
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> where( String fieldName, ComparatorOperator comparator, Object value ) {
        addTowhere(fieldName, comparator, value, null, null);
        return this;
    }

    /**
     * Inclui uma cláusula WHERE de BETWEEN
     * @param fieldName Nome da propriedade
     * @param comparator Comparador BETWEEN (apenas este é aceito para este método)
     * @param valueIni Valor inicial <b>(necessita implementar {@link Comparable})</b>
     * @param valueEnd Valor final <b>(necessita implementar {@link Comparable})</b>
     * @return objeto de consulta
     */
    @SuppressWarnings({ "rawtypes" }) // TODO: tentar resolver este warning
    public JpaCriteriaHelper<T> where( String fieldName, ComparatorOperator comparator, Comparable valueIni, Comparable valueEnd ) {
        addTowhere(fieldName, comparator, valueIni, valueEnd, null);
        return this;
    }

    /**
     * Inclui uma cláusula WHERE com um operador AND
     * @param fieldName Nome da propriedade
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> and( String fieldName, Object value ) {
        addTowhere(fieldName, ComparatorOperator.EQUAL, value, null, LogicalOperator.AND);
        return this;
    }

    /**
     * Inclui uma cláusula WHERE de BETWEEN após um operador AND
     * @param fieldName Nome da propriedade
     * @param comparator Comparador BETWEEN (apenas este é aceito para este método)
     * @param valueIni Valor inicial <b>(necessita implementar {@link Comparable})</b>
     * @param valueEnd Valor final <b>(necessita implementar {@link Comparable})</b>
     * @return objeto de consulta
     */
    @SuppressWarnings({ "rawtypes" }) // TODO: tentar resolver este warning
    public JpaCriteriaHelper<T> and( String fieldName, ComparatorOperator comparator, Comparable valueIni, Comparable valueEnd ) {
        wheres.add( new WhereEntry(fieldName, comparator, valueIni, valueEnd, LogicalOperator.AND) );
        return this;
    }

    /**
     * Inclui uma cláusula WHERE após um operador AND
     * @param fieldName Nome da propriedade
     * @param comparator Comparador <b>(Para {@link ComparatorOperator.GREATER_THAN} e {@link ComparatorOperator.GREATER_THAN}
     * é necessário que valor complemente {@link Comparable})</b>
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> and( String fieldName, ComparatorOperator comparator, Object value ) {
        wheres.add( new WhereEntry(fieldName, comparator, value, null, LogicalOperator.AND) );
        return this;
    }

    /**
     * Inclui uma cláusula WHERE com um operador AND
     * @param fieldName Nome da propriedade
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> or( String fieldName, Object value ) {
        addTowhere(fieldName, ComparatorOperator.EQUAL, value, null, LogicalOperator.OR);
        return this;
    }

    /**
     * Inclui uma cláusula WHERE de BETWEEN após um operador OR
     * @param fieldName Nome da propriedade
     * @param comparator Comparador BETWEEN (apenas este é aceito para este método)
     * @param valueIni Valor inicial <b>(necessita implementar {@link Comparable})</b>
     * @param valueEnd Valor final <b>(necessita implementar {@link Comparable})</b>
     * @return objeto de consulta
     */
    @SuppressWarnings({ "rawtypes" }) // TODO: tentar resolver este warning
    public JpaCriteriaHelper<T> or( String fieldName, ComparatorOperator comparator, Comparable valueIni, Comparable valueEnd ) {
        wheres.add( new WhereEntry(fieldName, comparator, valueIni, valueEnd, LogicalOperator.OR) );
        return this;
    }

    /**
     * Inclui uma cláusula WHERE após um operador OR
     * @param fieldName Nome da propriedade
     * @param comparator Comparador <b>(Para {@link ComparatorOperator.GREATER_THAN} e {@link ComparatorOperator.GREATER_THAN}
     * é necessário que valor complemente {@link Comparable})</b>
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> or( String fieldName, ComparatorOperator comparator, Object value ) {
        wheres.add( new WhereEntry(fieldName, comparator, value, null, LogicalOperator.OR) );
        return this;
    }

    /**
     * Inclui cláusula ORDER BY
     * @param fieldName Nome da propriedade
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> orderBy( String fieldName ) {
        orders.add( new OrderEntry(fieldName, OrderDirection.ASC) );
        return this;
    }

    /**
     * Define a última cláusula ORDER BY como ascedente
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> asc() {
        if ( ! orders.isEmpty() ) {
            throw new RuntimeException("Nenhum cláusula ORDER BY definida");
        }
        orders.get( orders.size() - 1 ).order = OrderDirection.ASC;
        return this;
    }

    /**
     * Define a última cláusula ORDER BY como descedente
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> desc() {
        if ( orders.isEmpty() ) {
            throw new RuntimeException("Nenhum cláusula ORDER BY definida");
        }
        orders.get( orders.size() - 1 ).order = OrderDirection.DESC;
        return this;
    }

    /**
     * Obtém lista com os resultados
     * @return Lista de resultados
     */
    public List<T> getResults() {
        CriteriaQuery<T> criteriaQuery = criteriaBuilder.createQuery(entityClass);
        Root<T> root                   = criteriaQuery.from(entityClass);
        setupQuery(criteriaQuery, root);

        // ORDER BY
        if ( ! orders.isEmpty() ) {
            ArrayList<Order> jpaOrders = new ArrayList<>();
            for (OrderEntry orderField : orders) {
                if ( orderField.order.equals(OrderDirection.ASC) ) {
                    jpaOrders.add( criteriaBuilder.asc( root.get(orderField.fieldName) ) );
                } else {
                    jpaOrders.add( criteriaBuilder.desc( root.get(orderField.fieldName) ) );
                }
            }
            criteriaQuery.orderBy( jpaOrders );
        }

        if ( pageNumber != null ) {
            return em.createQuery(criteriaQuery).setFirstResult( (pageNumber - 1) * pageSize ).setMaxResults(pageSize).getResultList();
        } else {
            return em.createQuery(criteriaQuery).getResultList();
        }
    }

    private void setupQuery(CriteriaQuery<T> criteriaQuery, Root<T> root) {
        // SELECT
        criteriaQuery.select(root);

        // WHERE
        if ( ! wheres.isEmpty() ) {
            criteriaQuery.where( getPredicates(root, wheres) );
        }
    }

    /**
     * Define o tamanho das páginas, em caso de paginação (tamanho padrão: 50)
     * @param pageSize Número de registros por página
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> setPageSize(Integer pageSize) {
        this.pageSize = pageSize;
        return this;
    }

    /**
     * Define a página que será retornada
     * <b>Se a página for informada, ativa a paginação de resultados</b>
     * Por padrão, não será efetuada paginação dos resultados
     * @param pageNumber Número da página (informe <b>null</b> para desativar a página)
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> page( Integer pageNumber ) {
        this.pageNumber = pageNumber;
        return this;
    }

    /**
     * Obtém apenas o primeiro registro do resultado da cpnsulta
     * @return O primeiro objeto retornado da consulta ou <b>null</b> se a consulta não retornar resultados
     */
    public T getFirstResult() {
        List<T> resultList = this.setPageSize(1).page(1).getResults();
        if ( resultList.isEmpty() ) {
            return null;
        } else {
            return resultList.get(0);
        }
    }

    /**
     * Obtém unico registro do resultado da consulta
     * @return O primeiro objeto retornado da consulta
     */
    public T getSingleResult() {
        CriteriaQuery<T> criteriaQuery = criteriaBuilder.createQuery(entityClass);
        Root<T> root                   = criteriaQuery.from(entityClass);
        setupQuery(criteriaQuery, root);

        return em.createQuery(criteriaQuery).getSingleResult();
    }

    /**
     * Verifica se a consulta retorna algum resultado
     * @return <li><b>true</b>: existem registros
     *         <li><b>false</b>: não existem registros
     */
    public boolean exists() {
        return this.getFirstResult() != null;
    }

    /**
     * Efetua a contagem dos registros da consulta
     * @return O número de registros retornados pela consulta
     */
    public long count() {
        CriteriaQuery<Long> criteriaQuery = criteriaBuilder.createQuery(Long.class);
        Root<T>                 rootCount = criteriaQuery.from(entityClass);

        criteriaQuery.select( criteriaBuilder.count( rootCount ) );

        if ( ! wheres.isEmpty() ) {
            criteriaQuery.where( getPredicates(rootCount, wheres) );
        }

        return em.createQuery( criteriaQuery ).getSingleResult();
    }

    private void addTowhere( String fieldName, ComparatorOperator comparator, Object valueIni, Object valueEnd, LogicalOperator logicalOperator ) {
        if ( ( comparator.equals(ComparatorOperator.GREATER_THAN) || comparator.equals(ComparatorOperator.LESS_THAN) )
                && ! (valueIni instanceof Comparable) ) {
            throw new RuntimeException("Para os tipos de operador "
                    + ComparatorOperator.GREATER_THAN + " e " + ComparatorOperator.LESS_THAN
                    + " é necessário que o objeto de valor implemente " + Comparable.class.getName() + ".");
        }

        if ( comparator.equals(ComparatorOperator.IN) && ! (valueIni instanceof Collection) ) {
            throw new RuntimeException("Para o tipo de operador " + ComparatorOperator.IN
                    + " é necessário que o objeto de valor implemente " + Collection.class.getName() + ".");
        }

        if ( valueEnd != null && ! comparator.equals( ComparatorOperator.BETWEEN ) ) {
            throw new RuntimeException("Quando informados dois valores, é obrigatório o uso de " + ComparatorOperator.BETWEEN);
        }

        if ( logicalOperator == null ) {
            logicalOperator = LogicalOperator.AND;
        }

        wheres.add( new WhereEntry(fieldName, comparator, valueIni, valueEnd, logicalOperator) );
    }

    @SuppressWarnings({ "unchecked", "rawtypes" }) // TODO: tentar retirar estes warnings
    private Predicate[] getPredicates( Root<T> root, List<WhereEntry> wheres ) {
        List<Predicate> predicates = new ArrayList<>();
        Predicate predMaster = null;

        for (WhereEntry whereEntry : wheres) {
            Predicate predicate;

            // --- OPERADOR DE COMPARAÇÃO ---
            switch (whereEntry.comparatorOperator) {
                case EQUAL:
                    if ( whereEntry.valueIni == null ) {
                        predicate = criteriaBuilder.isNull(root.get(whereEntry.fieldName));
                    } else {
                        predicate = criteriaBuilder.equal(root.get(whereEntry.fieldName), whereEntry.valueIni);
                    }
                    break;
                case NOT_EQUAL:
                    if ( whereEntry.valueIni == null ) {
                        predicate = criteriaBuilder.isNotNull(root.get(whereEntry.fieldName));
                    } else {
                        predicate = criteriaBuilder.notEqual(root.get(whereEntry.fieldName), whereEntry.valueIni);
                    }
                    break;
                case GREATER_THAN:
                    predicate = criteriaBuilder.greaterThan(root.get(whereEntry.fieldName), (Comparable) whereEntry.valueIni);
                    break;
                case LESS_THAN:
                    predicate = criteriaBuilder.lessThan(root.get(whereEntry.fieldName), (Comparable) whereEntry.valueIni);
                    break;
                case LIKE:
                    predicate = criteriaBuilder.like(root.get(whereEntry.fieldName), whereEntry.valueIni.toString());
                    break;
                case LIKE_IGNORE_CASE:
                    predicate = criteriaBuilder.like( criteriaBuilder.upper(root.get(whereEntry.fieldName)), whereEntry.valueIni.toString().toUpperCase() );
                    break;
                case IN:
                    predicate = root.get(whereEntry.fieldName).in( (Collection) whereEntry.valueIni);
                    break;
                case BETWEEN:
                    predicate = criteriaBuilder.between(root.get(whereEntry.fieldName), (Comparable) whereEntry.valueIni, (Comparable) whereEntry.valueEnd);
                    break;
                default:
                    throw new RuntimeException("Tipo de operador de comparação não conhecido: " + whereEntry.comparatorOperator);
            }

            if ( predMaster == null ) {
                predMaster = predicate;
            } else {
                // --- OPERADOR LÓGICO ---
                if ( whereEntry.logicalOperator != null ) {
                    switch ( whereEntry.logicalOperator ) {
                        case AND:
                            predMaster = criteriaBuilder.and( predMaster, predicate );
                            break;
                        case OR:
                            predMaster = criteriaBuilder.or( predMaster, predicate );
                            break;
                        default:
                            throw new RuntimeException("Tipo de operador lógico não conhecido: " + whereEntry.comparatorOperator);
                    }
                }
            }

        }
        predicates.add( predMaster );

        return predicates.toArray(new Predicate[] {});
    }

}
