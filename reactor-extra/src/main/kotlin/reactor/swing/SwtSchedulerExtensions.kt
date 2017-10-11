package reactor.swing

import org.eclipse.swt.widgets.Display
import reactor.core.scheduler.Scheduler

/**
 * @author Simon Baslé
 */
fun Display.toScheduler(): Scheduler = SwtScheduler.from(this)