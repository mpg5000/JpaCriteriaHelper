package org.jpahelper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;

import javax.persistence.EntityManager;
import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaDelete;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.CriteriaUpdate;
import javax.persistence.criteria.From;
import javax.persistence.criteria.Order;
import javax.persistence.criteria.Path;
import javax.persistence.criteria.Predicate;
import javax.persistence.criteria.Root;
import javax.persistence.criteria.Selection;
import javax.persistence.metamodel.ListAttribute;

/**
 * Classe utilitária para facilitação das chamadas mais simples à JPA utilizando CriteriaBuilder.
 * @author mauricio.guzinski e pietro.biasuz
 *
 */
public class JpaCriteriaHelper<T> {

    public enum ComparatorOperator { EQUAL, NOT_EQUAL, LIKE, LIKE_IGNORE_CASE, BETWEEN, GREATER_THAN, LESS_THAN, IN };
    public enum LogicalOperator { AND, OR };
    public enum OrderDirection { ASC, DESC };
    private enum SqlOperation { SELECT, UPDATE };

    private static final Integer DEFAULT_PAGE_SIZE = 50;

    private EntityManager em;

    private CriteriaBuilder criteriaBuilder;

    private List<WhereEntry> wheres = new ArrayList<>();

    private List<OrderEntry> orders = new ArrayList<>();

    private Map<List<String>, Object> updates = new HashMap<>();

    private Integer pageSize = DEFAULT_PAGE_SIZE;

    private Integer pageNumber;

    private Class<T> entityClass;

    private List<String> directFetches = new ArrayList<>();

    private List<ListFetch<?>> listFetches = new ArrayList<>();

    private Map<Path<?>, From<?, ?>> joinsMap = new HashMap<>();

    private SqlOperation sqlOperation;

    private class ListFetch<E> {
        private String attribute;
        private Class<E> clazz;

        public ListFetch(String attribute, Class<E> clazz) {
            this.attribute = attribute;
            this.clazz = clazz;
        }
    }

    /**
     * Define nome entradas da cláusula WHERE
     *
     */
    private class WhereEntry {

        private List<String> fieldNames;

        private ComparatorOperator comparatorOperator;

        private Object valueIni;

        private Object valueEnd;

        private LogicalOperator logicalOperator;

        public WhereEntry(List<String> fieldNames
                ,ComparatorOperator comparatorOperator
                ,Object valueIni
                ,Object valueEnd
                ,LogicalOperator logicalOperator) {
            this.fieldNames          = fieldNames;
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

        private List<String> fieldNames;

        private OrderDirection order;

        public OrderEntry(List<String> fieldNames, OrderDirection order) {
            this.fieldNames = fieldNames;
            this.order = order;
        }
    }

    private JpaCriteriaHelper( EntityManager em, Class<T> entityClass, SqlOperation sqlOperation ) {
        this.em              = em;
        this.entityClass     = entityClass;
        this.criteriaBuilder = em.getCriteriaBuilder();
        this.sqlOperation    = sqlOperation;
    }

    /**
     * Cria o objeto de consulta para executar a query
     * @param em EntityManager
     * @param entityClazz Classe de destino
     * @return objeto de consulta
     */
    public static <X> JpaCriteriaHelper<X> select( EntityManager em, Class<X> entityClazz ) {
        return new JpaCriteriaHelper<>( em, entityClazz, SqlOperation.SELECT );
    }

    /**
     * Cria o objeto para update
     * @param em EntityManager
     * @param entityClazz Classe de destino
     * @return objeto de update
     */
    public static <X> JpaCriteriaHelper<X> update( EntityManager em, Class<X> entityClazz ) {
        return new JpaCriteriaHelper<>( em, entityClazz, SqlOperation.UPDATE );
    }

    /**
     * Atribui valor a um campo (em uma operação de update)
     * @param fieldName Nome da propriedade
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> set( String fieldName, Object value ) {
        return set( Arrays.asList(fieldName), value );
    }

    /**
     * Atribui valor a um campo (em uma operação de update)
     * @param fieldNames Nome da propriedade
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> set( List<String> fieldNames, Object value ) {
        demandsOperation(SqlOperation.UPDATE);
        updates.put(fieldNames, value);
        return this;
    }

    /**
     * Inclui uma clausula WHERE com operador {@link ComparatorOperator.EQUAL} implicito
     * @param fieldName fieldName Nome da propriedade
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> where( String fieldName, Object value ) {
        return where(fieldName, ComparatorOperator.EQUAL, value);
    }

    /**
     * Inclui uma clausula WHERE com operador {@link ComparatorOperator.EQUAL} implicito
     * @param fieldNames fieldName Nome da propriedade
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> where( List<String> fieldNames, Object value ) {
        return where(fieldNames, ComparatorOperator.EQUAL, value);
    }

    /**
     * Inclui uma clausula WHERE
     * @param fieldName Nome da propriedade
     * @param comparator Comparador <b>(Para {@link ComparatorOperator.GREATER_THAN} e {@link ComparatorOperator.GREATER_THAN}
     * é necessário que valor complemente {@link Comparable})</b>
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> where( String fieldName, ComparatorOperator comparator, Object value ) {
        addTowhere(Arrays.asList(fieldName), comparator, value, null, null);
        return this;
    }

    /**
     * Inclui uma clausula WHERE
     * @param fieldNames Nome da propriedade
     * @param comparator Comparador <b>(Para {@link ComparatorOperator.GREATER_THAN} e {@link ComparatorOperator.GREATER_THAN}
     * é necessário que valor complemente {@link Comparable})</b>
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> where( List<String> fieldNames, ComparatorOperator comparator, Object value ) {
        addTowhere(fieldNames, comparator, value, null, null);
        return this;
    }

    /**
     * Inclui uma clausula WHERE de BETWEEN
     * @param fieldName Nome da propriedade
     * @param comparator Comparador BETWEEN (apenas este é aceito para este método)
     * @param valueIni Valor inicial <b>(necessita implementar {@link Comparable})</b>
     * @param valueEnd Valor final <b>(necessita implementar {@link Comparable})</b>
     * @return objeto de consulta
     */
    @SuppressWarnings({ "rawtypes" }) // TODO: tentar resolver este warning
    public JpaCriteriaHelper<T> where( String fieldName, ComparatorOperator comparator, Comparable valueIni, Comparable valueEnd ) {
        addTowhere(Arrays.asList(fieldName), comparator, valueIni, valueEnd, null);
        return this;
    }

    /**
     * Inclui uma clausula WHERE de BETWEEN após um operador AND
     * @param fieldNames Nome da propriedade
     * @param comparator Comparador BETWEEN (apenas este é aceito para este método)
     * @param valueIni Valor inicial <b>(necessita implementar {@link Comparable})</b>
     * @param valueEnd Valor final <b>(necessita implementar {@link Comparable})</b>
     * @return objeto de consulta
     */
    @SuppressWarnings({ "rawtypes" }) // TODO: tentar resolver este warning
    public JpaCriteriaHelper<T> where( List<String> fieldNames, ComparatorOperator comparator, Comparable valueIni, Comparable valueEnd ) {
        wheres.add( new WhereEntry(fieldNames, comparator, valueIni, valueEnd, LogicalOperator.AND) );
        return this;
    }

    /**
     * Inclui uma clausula WHERE com um operador AND
     * @param fieldName Nome da propriedade
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> and( String fieldName, Object value ) {
        addTowhere(Arrays.asList(fieldName), ComparatorOperator.EQUAL, value, null, LogicalOperator.AND);
        return this;
    }

    /**
     * Inclui uma clausula WHERE com um operador AND
     * @param fieldNames Nome da propriedade
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> and( List<String> fieldNames, Object value ) {
        addTowhere(fieldNames, ComparatorOperator.EQUAL, value, null, LogicalOperator.AND);
        return this;
    }

    /**
     * Inclui uma clausula WHERE de BETWEEN após um operador AND
     * @param fieldName Nome da propriedade
     * @param comparator Comparador BETWEEN (apenas este é aceito para este método)
     * @param valueIni Valor inicial <b>(necessita implementar {@link Comparable})</b>
     * @param valueEnd Valor final <b>(necessita implementar {@link Comparable})</b>
     * @return objeto de consulta
     */
    @SuppressWarnings({ "rawtypes" }) // TODO: tentar resolver este warning
    public JpaCriteriaHelper<T> and( String fieldName, ComparatorOperator comparator, Comparable valueIni, Comparable valueEnd ) {
        wheres.add( new WhereEntry(Arrays.asList(fieldName), comparator, valueIni, valueEnd, LogicalOperator.AND) );
        return this;
    }

    /**
     * Inclui uma clausula WHERE de BETWEEN após um operador AND
     * @param fieldNames Nome das propriedades
     * @param comparator Comparador BETWEEN (apenas este é aceito para este método)
     * @param valueIni Valor inicial <b>(necessita implementar {@link Comparable})</b>
     * @param valueEnd Valor final <b>(necessita implementar {@link Comparable})</b>
     * @return objeto de consulta
     */
    @SuppressWarnings({ "rawtypes" }) // TODO: tentar resolver este warning
    public JpaCriteriaHelper<T> and( List<String> fieldNames, ComparatorOperator comparator, Comparable valueIni, Comparable valueEnd ) {
        wheres.add( new WhereEntry(fieldNames, comparator, valueIni, valueEnd, LogicalOperator.AND) );
        return this;
    }

    /**
     * Inclui uma clausula WHERE após um operador AND
     * @param fieldName Nome da propriedade
     * @param comparator Comparador <b>(Para {@link ComparatorOperator.GREATER_THAN} e {@link ComparatorOperator.GREATER_THAN}
     * é necessário que valor complemente {@link Comparable})</b>
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> and( String fieldName, ComparatorOperator comparator, Object value ) {
        wheres.add( new WhereEntry(Arrays.asList(fieldName), comparator, value, null, LogicalOperator.AND) );
        return this;
    }

    /**
     * Inclui uma clausula WHERE após um operador AND
     * @param fieldNames Nome das propriedades
     * @param comparator Comparador <b>(Para {@link ComparatorOperator.GREATER_THAN} e {@link ComparatorOperator.GREATER_THAN}
     * é necessário que valor complemente {@link Comparable})</b>
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> and( List<String> fieldNames, ComparatorOperator comparator, Object value ) {
        wheres.add( new WhereEntry(fieldNames, comparator, value, null, LogicalOperator.AND) );
        return this;
    }

    /**
     * Inclui uma clausula WHERE com um operador AND
     * @param fieldName Nome da propriedade
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> or( String fieldName, Object value ) {
        addTowhere(Arrays.asList(fieldName), ComparatorOperator.EQUAL, value, null, LogicalOperator.OR);
        return this;
    }

    /**
     * Inclui uma clausula WHERE de BETWEEN após um operador OR
     * @param fieldName Nome da propriedade
     * @param comparator Comparador BETWEEN (apenas este é aceito para este método)
     * @param valueIni Valor inicial <b>(necessita implementar {@link Comparable})</b>
     * @param valueEnd Valor final <b>(necessita implementar {@link Comparable})</b>
     * @return objeto de consulta
     */
    @SuppressWarnings({ "rawtypes" }) // TODO: tentar resolver este warning
    public JpaCriteriaHelper<T> or( String fieldName, ComparatorOperator comparator, Comparable valueIni, Comparable valueEnd ) {
        wheres.add( new WhereEntry(Arrays.asList(fieldName), comparator, valueIni, valueEnd, LogicalOperator.OR) );
        return this;
    }

    /**
     * Inclui uma clausula WHERE de BETWEEN após um operador OR
     * @param fieldNames Nome das propriedades
     * @param comparator Comparador BETWEEN (apenas este é aceito para este método)
     * @param valueIni Valor inicial <b>(necessita implementar {@link Comparable})</b>
     * @param valueEnd Valor final <b>(necessita implementar {@link Comparable})</b>
     * @return objeto de consulta
     */
    @SuppressWarnings({ "rawtypes" }) // TODO: tentar resolver este warning
    public JpaCriteriaHelper<T> or( List<String> fieldNames, ComparatorOperator comparator, Comparable valueIni, Comparable valueEnd ) {
        wheres.add( new WhereEntry(fieldNames, comparator, valueIni, valueEnd, LogicalOperator.OR) );
        return this;
    }

    /**
     * Inclui uma clausula WHERE após um operador OR
     * @param fieldName Nome da propriedade
     * @param comparator Comparador <b>(Para {@link ComparatorOperator.GREATER_THAN} e {@link ComparatorOperator.GREATER_THAN}
     * é necessário que valor complemente {@link Comparable})</b>
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> or( String fieldName, ComparatorOperator comparator, Object value ) {
        wheres.add( new WhereEntry(Arrays.asList(fieldName), comparator, value, null, LogicalOperator.OR) );
        return this;
    }

    /**
     * Inclui uma clausula WHERE após um operador OR
     * @param fieldNames Nome das propriedades
     * @param comparator Comparador <b>(Para {@link ComparatorOperator.GREATER_THAN} e {@link ComparatorOperator.GREATER_THAN}
     * é necessário que valor complemente {@link Comparable})</b>
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> or( List<String> fieldNames, ComparatorOperator comparator, Object value ) {
        wheres.add( new WhereEntry(fieldNames, comparator, value, null, LogicalOperator.OR) );
        return this;
    }

    /**
     * Inclui uma clausula WHERE após um operador OR
     * @param fieldNames Nome das propriedades
     * @param value Valor
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> or( List<String> fieldNames, Object value ) {
        wheres.add( new WhereEntry(fieldNames, ComparatorOperator.EQUAL, value, null, LogicalOperator.OR) );
        return this;
    }

    /**
     * Inclui clausula ORDER BY
     * @param fieldNames Nome da propriedade
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> orderBy( String ... fieldNames ) {
        demandsOperation(SqlOperation.SELECT);
        orders.add( new OrderEntry(Arrays.asList(fieldNames), OrderDirection.ASC) );
        return this;
    }

    /**
     * Define a ultima clausula ORDER BY como ascedente
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> asc() {
        demandsOperation(SqlOperation.SELECT);
        if ( ! orders.isEmpty() ) {
            throw new RuntimeException("Nenhum cláusula ORDER BY definida");
        }
        orders.get( orders.size() - 1 ).order = OrderDirection.ASC;
        return this;
    }

    /**
     * Define a ultima clausula ORDER BY como descedente
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> desc() {
        demandsOperation(SqlOperation.SELECT);
        if ( orders.isEmpty() ) {
            throw new RuntimeException("Nenhum cláusula ORDER BY definida");
        }
        orders.get( orders.size() - 1 ).order = OrderDirection.DESC;
        return this;
    }

    /**
     * Obtem lista com os resultados
     * @return Lista de resultados
     */
    public List<T> getResults() {
        demandsOperation(SqlOperation.SELECT);
        CriteriaQuery<T> criteriaQuery = criteriaBuilder.createQuery(entityClass);
        Root<T> root = criteriaQuery.from(entityClass);

        criteriaQuery.select(root);

        setupQuery(criteriaQuery, root);

        TypedQuery<T> typedQuery = em.createQuery(criteriaQuery);

        setupPagination(typedQuery);

        return typedQuery.getResultList();
    }

    private <E, Y> void orderBy(CriteriaQuery<E> criteriaQuery, Root<Y> root) {
        if (!orders.isEmpty()) {
            ArrayList<Order> jpaOrders = new ArrayList<>();
            for (OrderEntry orderField : orders) {
                if (orderField.order.equals(OrderDirection.ASC)) {
                    jpaOrders.add(criteriaBuilder.asc(getPath(orderField.fieldNames, root)));
                } else {
                    jpaOrders.add(criteriaBuilder.desc(getPath(orderField.fieldNames, root)));
                }
            }
            criteriaQuery.orderBy(jpaOrders);
        }
    }

    public void delete() {
        CriteriaDelete<T> criteriaDelete = criteriaBuilder.createCriteriaDelete(entityClass);

        Root<T> root = criteriaDelete.from(entityClass);

        if (!wheres.isEmpty()) {
            criteriaDelete.where( getPredicates(root, wheres) );
        }

        em.createQuery(criteriaDelete).executeUpdate();
    }

    private <C, R> void setupQuery(CriteriaQuery<C> criteriaQuery, Root<R> root) {
        //FETCH JOINS
        directFetch(root);

        listFetch(root);

        if (!wheres.isEmpty()) {
            criteriaQuery.where(getPredicates(root, wheres));
        }

        orderBy(criteriaQuery, root);
    }

    private <R> void listFetch(Root<R> root) {
        for (JpaCriteriaHelper<T>.ListFetch<?> listFetch : listFetches) {
            ListAttribute<? super R, ?> listAttribute = root.getModel().getList(listFetch.attribute, listFetch.clazz);
            root.fetch(listAttribute);
        }
    }

    private <R> void directFetch(Root<R> root) {
        for (String fetch : directFetches) {
            root.fetch(fetch);
        }
    }

    /**
     * Define o tamanho das páginas, em caso de paginação (tamanho padrão: 50)
     * @param pageSize Número de registros por página
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> setPageSize(Integer pageSize) {
        demandsOperation(SqlOperation.SELECT);
        this.pageSize = pageSize;
        return this;
    }

    /**
     * Define a pagina que serah retornada
     * <b>Se a página for informada, ativa a paginação de resultados</b>
     * Por padrão, não será efetuada paginação dos resultados
     * @param pageNumber Número da página (informe <b>null</b> para desativar a página)
     * @return objeto de consulta
     */
    public JpaCriteriaHelper<T> page( Integer pageNumber ) {
        demandsOperation(SqlOperation.SELECT);
        this.pageNumber = pageNumber;
        return this;
    }

    /**
     * Obtem apenas o primeiro registro do resultado da consulta
     * @return O primeiro objeto retornado da consulta ou <b>null</b> se a consulta não retornar resultados
     */
    public T getFirstResult() {
        demandsOperation(SqlOperation.SELECT);
        List<T> resultList = this.setPageSize(1).page(1).getResults();
        if ( resultList.isEmpty() ) {
            return null;
        } else {
            return resultList.get(0);
        }
    }

    /**
     * Obtém apenas o primeiro registro do resultado da cpnsulta
     * @return O primeiro objeto retornado da consulta ou <b>null</b> se a consulta não retornar resultados
     */
    public Optional<T> getFirstResultOpt() {
        demandsOperation(SqlOperation.SELECT);
        T firstResult = getFirstResult();

        if (firstResult == null) {
            return Optional.empty();
        } else {
            return Optional.of(firstResult);
        }
    }

    /**
     * Obtem unico registro do resultado da consulta
     * @return O primeiro objeto retornado da consulta
     */
    public T getSingleResult() {
        demandsOperation(SqlOperation.SELECT);
        CriteriaQuery<T> criteriaQuery = criteriaBuilder.createQuery(entityClass);
        Root<T> root = criteriaQuery.from(entityClass);
        criteriaQuery.select(root);

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
     * @return numero de registros retornados pela consulta
     */
    public long count() {
        demandsOperation(SqlOperation.SELECT);
        CriteriaQuery<Long> criteriaQuery = criteriaBuilder.createQuery(Long.class);
        Root<T>                 rootCount = criteriaQuery.from(entityClass);

        criteriaQuery.select( criteriaBuilder.count( rootCount ) );

        if ( ! wheres.isEmpty() ) {
            criteriaQuery.where( getPredicates(rootCount, wheres) );
        }

        return em.createQuery( criteriaQuery ).getSingleResult();
    }

    /**
     * Efetua operação de UPDATE
     * @return
     */
    @SuppressWarnings({ "rawtypes", "unchecked" }) // TODO: tentar retirar estes warnings
    public int execute() {
        demandsOperation(SqlOperation.UPDATE);
        CriteriaUpdate<T> criteriaUpdate = criteriaBuilder.createCriteriaUpdate(entityClass);
        Root<T> rootUpdate               = criteriaUpdate.from(entityClass);

        if ( ! wheres.isEmpty() ) {
            criteriaUpdate.where( getPredicates(rootUpdate, wheres) );
        }

        if ( updates.isEmpty() ) {
            throw new RuntimeException("Nenhum campo de update foi informado.");
        }

        for (Entry<List<String>, Object> updateEntry : updates.entrySet()) {
            Path path = getPath(updateEntry.getKey(), rootUpdate);
            criteriaUpdate.set(path, updateEntry.getValue());
        }

        return em.createQuery( criteriaUpdate ).executeUpdate();
    }

    private void addTowhere( List<String> fieldNames, ComparatorOperator comparator, Object valueIni, Object valueEnd, LogicalOperator logicalOperator ) {
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

        wheres.add( new WhereEntry(fieldNames, comparator, valueIni, valueEnd, logicalOperator) );
    }

    @SuppressWarnings({ "unchecked", "rawtypes" }) // TODO: tentar retirar estes warnings
    private <E> Predicate[] getPredicates( Root<E> root, List<WhereEntry> wheres ) {
        List<Predicate> predicates = new ArrayList<>();
        Predicate predMaster = null;

        for (WhereEntry whereEntry : wheres) {
            Predicate predicate;

            // --- OPERADOR DE COMPARAÇÃO ---
            Path path = getPath(whereEntry.fieldNames, root);
            switch (whereEntry.comparatorOperator) {
                case EQUAL:
                    if ( whereEntry.valueIni == null ) {
                        predicate = criteriaBuilder.isNull(path);
                    } else {
                        predicate = criteriaBuilder.equal(path, whereEntry.valueIni);
                    }
                    break;
                case NOT_EQUAL:
                    if ( whereEntry.valueIni == null ) {
                        predicate = criteriaBuilder.isNotNull(path);
                    } else {
                        predicate = criteriaBuilder.notEqual(path, whereEntry.valueIni);
                    }
                    break;
                case GREATER_THAN:
                    predicate = criteriaBuilder.greaterThan(path, (Comparable) whereEntry.valueIni);
                    break;
                case LESS_THAN:
                    predicate = criteriaBuilder.lessThan(path, (Comparable) whereEntry.valueIni);
                    break;
                case LIKE:
                    predicate = criteriaBuilder.like(path, whereEntry.valueIni.toString());
                    break;
                case LIKE_IGNORE_CASE:
                    predicate = criteriaBuilder.like( criteriaBuilder.upper(path), whereEntry.valueIni.toString().toUpperCase() );
                    break;
                case IN:
                    predicate = path.in( (Collection) whereEntry.valueIni);
                    break;
                case BETWEEN:
                    predicate = criteriaBuilder.between(path, (Comparable) whereEntry.valueIni, (Comparable) whereEntry.valueEnd);
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

    // TODO: testar se estah fazendo JOIN corretamente para multiplos niveis
    private <E> Path<?> getPath(List<String> fieldNames, Root<E> root) {
        javax.persistence.criteria.Path<?> entity = root;

        for (String fieldName : fieldNames) {
            Path<Object> fieldAsPath = entity.get(fieldName);
            if ( Collection.class.isAssignableFrom( fieldAsPath.getJavaType() ) ) {
                if ( ! joinsMap.containsKey(fieldAsPath) ) {
                    joinsMap.put(fieldAsPath, ((From<?, ?>) entity).join(fieldName));
                }
                entity = joinsMap.get(fieldAsPath);
            } else {
                entity = entity.get(fieldName);
            }
        }

        return entity;
    }

    // TODO: demandsOperation(SqlOperation.SELECT); ?
    public JpaCriteriaHelper<T> fetch(String property) {
        this.directFetches.add(property);

        return this;
    }

    // TODO: demandsOperation(SqlOperation.SELECT); ?
    public <E> JpaCriteriaHelper<T> fetch(String fetch, Class<E> clazz) {
        this.listFetches.add(new ListFetch<>(fetch, clazz));

        return this;
    }

    // TODO: necessário falar com Pietro ??
    public static <T> JpaCriteriaHelper<T> create(EntityManager em, Class<T> entityClazz) {
        return new JpaCriteriaHelper<>( em, entityClazz, SqlOperation.SELECT );
    }

    /**
     * Obtem as columns especificadas na classe tupleClazz E a partir da entityClass T.
     *
     * <p>
     * A classe tupleClazz precisa possuir um construtor que respeite a ordem das columns informadas.
     *
     * @param tupleClazz POJO de retorno para a busca.
     * @param columns colunas que se deseja trazer da entityClass T.
     *
     * @return
     */
    public <C> List<C> getTupleResults(Class<C> tupleClazz, List<String> columns) {
        demandsOperation(SqlOperation.SELECT);
        Objects.requireNonNull(tupleClazz);
        Objects.requireNonNull(columns);

        CriteriaQuery<C> cq = em.getCriteriaBuilder().createQuery(tupleClazz);
        Root<T> root = cq.from(entityClass);

        setupQuery(cq, root);

        if (!columns.isEmpty()) {
            List<Selection<?>> selections = new ArrayList<>();

            for (String column : columns) {
                selections.add(root.get(column));
            }

            Selection<?>[] selectionsArray = selections.toArray(new Selection<?>[selections.size()]);
            cq.multiselect(selectionsArray);
        }

        TypedQuery<C> typedQuery = em.createQuery(cq);

        setupPagination(typedQuery);

        return typedQuery.getResultList();
    }

    private <E> void setupPagination(TypedQuery<E> tq) {
        if (pageNumber != null) {
            tq.setFirstResult((pageNumber - 1) * pageSize).setMaxResults(pageSize);
        }
    }

    /**
     * Confere se a operação atual do objeto é a operação esperada. Lança uma exceção se a operação atual não for igual à esperada.
     * @param sqlOperation Operação esperada
     */
    private void demandsOperation( SqlOperation sqlOperation ) {
        if ( ! this.sqlOperation.equals( sqlOperation ) ) {
            throw new RuntimeException("Chamada de método inválida para a operação " + this.sqlOperation.name() + ".");
        }
    }

}
