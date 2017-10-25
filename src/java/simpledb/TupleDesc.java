package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

	private List<TDItem> _itemList = new ArrayList<>();
	private Map<String, Integer> _nameindexMap = new HashMap<>();
	
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return _itemList.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	if(typeAr == null || fieldAr == null){
    		throw new NullPointerException("type or filed is null!");
    	}
    	
    	if(typeAr.length != fieldAr.length){
    		throw new IllegalArgumentException("type and filed is not macth");
    	}
    	
    	for(int i = 0;i < typeAr.length; ++i){
    		TDItem tmp = new TDItem(typeAr[i],fieldAr[i]);
    		_itemList.add(tmp);
    		int index = _itemList.size();
    		_nameindexMap.put(fieldAr[i], index-1);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	if(typeAr == null){
    		throw new NullPointerException("type is null!");
    	}
    	
    	if(typeAr.length == 0){
    		throw new IllegalArgumentException("type list is 0 element");
    	}
    	
    	for(int i = 0;i < typeAr.length; ++i){
    		TDItem tmp = new TDItem(typeAr[i],null);
    		_itemList.add(tmp);
    	}
    }

    public TupleDesc() {
		// TODO Auto-generated constructor stub
	}

	/**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return _itemList.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if(i < 0 || i >= _itemList.size()){
    		throw new NoSuchElementException();
    	}
        return _itemList.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
    	if(i < 0 || i >= _itemList.size()){
    		throw new NoSuchElementException();
    	}
        return _itemList.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
    	Integer index = _nameindexMap.get(name);
    	if(index == null){
    		throw new NoSuchElementException();
    	}
        return index;
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int size = 0;
    	for(TDItem tmp : _itemList){
    		size += tmp.fieldType.getLen();
    	}
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	TupleDesc tmp = new TupleDesc();
    	tmp.addTupleDesc(td1);
    	tmp.addTupleDesc(td2);
        return tmp;
    }

    private List<TDItem> getItemList(){
    	return _itemList;
    }
    
    private Map<String, Integer> getNameMap(){
    	return _nameindexMap;
    }
    
    private void addTupleDesc(TupleDesc desc){
    	if(desc == null){
    		throw new NullPointerException();
    	}
    	
    	_itemList.addAll(desc.getItemList());
    	_nameindexMap.putAll(desc.getNameMap());
    }
    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
    	if(o == null){
    		return false;
    	}
    	
    	if(!(o instanceof TupleDesc)){
    		return false;
    	}
    	
    	TupleDesc tmp = (TupleDesc)o;
    	if(this.numFields() != tmp.numFields()){
    		return false;
    	}
    	
    	for(int i=0; i < this.numFields(); ++i){
    		if(this.getFieldType(i) != tmp.getFieldType(i)){
    			return false;
    		}
    	}
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
    	StringBuilder info = new StringBuilder();
    	boolean isFirst = true;
    	for(TDItem tmp : _itemList){
    		if(!isFirst){
    			info.append(",");
    		}
    		info.append(tmp.fieldType.toString());
    		info.append("(");
    		info.append(tmp.fieldName);
    		info.append("");
    		isFirst = false;
    	}
        return "";
    }
}
