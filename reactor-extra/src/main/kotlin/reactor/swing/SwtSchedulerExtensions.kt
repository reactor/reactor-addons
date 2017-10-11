package reactor.swing

import org.eclipse.swt.widgets.Display
import reactor.core.scheduler.Scheduler

/**
 * Extension to convert a SWT Display to a [Scheduler].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
fun Display.toScheduler(): Scheduler = SwtScheduler.from(this)