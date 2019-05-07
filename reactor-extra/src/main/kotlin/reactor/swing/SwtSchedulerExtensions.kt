package reactor.swing

import org.eclipse.swt.widgets.Display
import reactor.core.scheduler.Scheduler

/**
 * Extension to convert a SWT Display to a [Scheduler].
 *
 * @author Simon Baslé
 * @since 3.1.1
 */
@Deprecated("To be removed in 3.3.0.RELEASE, replaced by module reactor-kotlin-extensions",
        ReplaceWith("toScheduler()", "reactor.kotlin.extra.swing.toScheduler"))
fun Display.toScheduler(): Scheduler = SwtScheduler.from(this)