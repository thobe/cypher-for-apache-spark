package org.opencypher.spark.examples;

import java.util.concurrent.TimeUnit;

class Terminal
{
    private static final long TYPING_DELAY = 20;
    private static final long CURSOR_DELAY = 250;

    static void write( String string, String linePrefix ) throws InterruptedException
    {
        for ( int i = 0, cp; i < string.length(); i += Character.charCount( cp ) )
        {
            cp = string.codePointAt( i );
            System.out.append( string, i, i + Character.charCount( cp ) );
            System.out.flush();
            if ( cp == '\n' )
            {
                System.out.print( linePrefix );
                System.out.flush();
            }
            Thread.sleep( TYPING_DELAY );
        }
        System.out.println();
        System.out.println( linePrefix );
        System.out.println();
    }

    static void blink( String prompt, long time, TimeUnit unit ) throws InterruptedException
    {
        System.out.print( prompt );
        for ( long end = System.currentTimeMillis() + unit.toMillis( time ); System.currentTimeMillis() < end; )
        {
            System.out.print( "_\b" );
            System.out.flush();
            System.out.print( " \b" );
            System.out.flush();
        }
    }
}
