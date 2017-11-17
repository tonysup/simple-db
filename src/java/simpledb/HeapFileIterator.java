package simpledb;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class HeapFileIterator extends AbstractDbFileIterator {
    
	private HeapPage heapPage;
	private Iterator<Tuple> iterator;
	boolean isInit = false;
	public HeapFileIterator(HeapPage info){
		this.heapPage = info;
	}
	
	@Override
	public void open() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		iterator = this.heapPage.iterator();
		isInit = true;
	}

	@Override
	public void rewind() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		iterator = this.heapPage.iterator();
	}

	@Override
	public Tuple readNext() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub
		return iterator.next();
	}
	
	@Override
	public boolean hasNext() throws DbException, TransactionAbortedException {
		if(!isInit){
			return false;
		}
		return iterator.hasNext();
	}
	
	@Override
	public Tuple next() throws DbException, TransactionAbortedException{
		if(!isInit){
			throw new NoSuchElementException();
		}
		
		return iterator.next();
	}
	
	@Override
	public void close() {
		isInit = false;
	}
}
