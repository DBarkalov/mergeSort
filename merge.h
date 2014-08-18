#include <boost/bind.hpp>
#include <boost/asio.hpp>
#include <boost/thread.hpp>


// слияние двух массивов в один
// описание многопоточного варианта в Кормен 3-е издание стр. 788
class Merge
{
public:
	Merge(boost::asio::io_service& ios, int num_threads):io_service(ios), number_of_threads(num_threads)
	{
	}

public:

	// многопоточное слияние
  template< class _Type>
  void StartParallelMerge( _Type* t, int p1, int r1, int p2, int r2, _Type* a, int p3 )
  {	      
	io_service.reset();

	 // первое задание в очередь
    io_service.post(boost::bind(&Merge::ParallelMerge<_Type>, this, t, p1, r1, p2, r2,a, p3));

	// если неправильное значение
	if(number_of_threads < 1) number_of_threads = 2;

	// запуск рабочих потоков
	boost::thread_group tg;	
	for(int i=0;i<=number_of_threads;i++)
	{
		tg.create_thread(boost::bind(&boost::asio::io_service::run, &io_service));
	}
	// ждем завершения
    tg.join_all(); 
  }


private:

template< class _Type >
void SerialMerge( const _Type* a_start, const _Type* a_end, const _Type* b_start, const _Type* b_end, _Type* dst )
{
	while( a_start < a_end && b_start < b_end )
	{
		if ( *a_start <= *b_start )	
		{
			*dst++ = *a_start++;	
		}
		else
		{
			*dst++ = *b_start++;
		}
	}

	while( a_start < a_end )
	{
		*dst++ = *a_start++;
	}

	while( b_start < b_end )
	{
		*dst++ = *b_start++;
	}
}


template< class _Type >
int binary_search( _Type value, const _Type* a, int left, int right )
{
	long low  = left;
	long high = __max( left, right + 1 );
	while( low < high )
	{
		long mid = ( low + high ) / 2;
		if ( value <= a[ mid ] )	high = mid;
		else						low  = mid + 1;	
													
													
	}
	return high;
}

template < class Item >
void exchange( Item& A, Item& B )
{
	Item t = A;
	A = B;
	B = t;
}



template< class _Type >
void ParallelMerge( _Type* t, int p1, int r1, int p2, int r2, _Type* a, int p3 )
{
	int length1 = r1 - p1 + 1;
	int length2 = r2 - p2 + 1;

	if ( length1 < length2 ) 
	{
		exchange(      p1,      p2 );
		exchange(      r1,      r2 );
		exchange( length1, length2 );
	}
	if ( length1 == 0 )	
	{
		return;
	}

	// для не больших массивов работа в этом потоке
	if (( length1 + length2 ) <= 10000 ) 
	{
		SerialMerge( &t[ p1 ], &t[ p1 + length1 ], &t[ p2 ], &t[ p2 + length2 ], &a[ p3 ] );
	}
	else 
	{
		int q1 = ( p1 + r1 ) / 2;						// индекс середины первого массива
		int q2 = binary_search( t[ q1 ], t, p2, r2 );	// во втором массиве находим индекс элемента который >= t[ q1 ]
		int q3 = p3 + ( q1 - p1 ) + ( q2 - p2 );		// 
		a[ q3 ] = t[ q1 ];								//
        
		io_service.post(boost::bind(&Merge::ParallelMerge<_Type>, this, t, p1,     q1 - 1, p2, q2 - 1, a, p3));
		io_service.post(boost::bind(&Merge::ParallelMerge<_Type>, this, t, q1 + 1, r1,     q2, r2,     a, q3 + 1));        
	}
}

private:
	boost::asio::io_service& io_service;
	int number_of_threads;  // число рабочих потоков в пулле

};
