/** Root-scoped controller for the right Sidebar's Session content. */
import { useLayoutEffect, useState } from 'react'
import { createPortal } from 'react-dom'
import type { HostObservable, InjectFace, PropsRenderSlots, PropsRuntime } from '@deepseek-ai/dsh-client-ui-slots'
import type { SessionReference } from '@deepseek-ai/dsh-api-session-controller/client'
import type { SidebarSessionViewSnapshot } from '../session-views.ts'
import type {} from '../contract/slots.ts'
import css from './SidebarRight.module.css'

/** Root-only retained Session targets and their committed mount lifetimes. */
export interface RightbarRootInjected {
  readonly hooks: {
    readonly views: HostObservable<readonly SidebarSessionViewSnapshot[]>
    readonly externalHost: HostObservable<{ element: HTMLElement; sessionId: string } | undefined>
  }
  readonly mountView: (reference: SessionReference) => () => void
}

type RootProps = PropsRuntime<'rightbar'> & PropsRenderSlots<'rightbar.session'> & InjectFace<RightbarRootInjected>

function SessionView({ view, visible, SessionProvider, renderSlot, mountView, width, viewportWidth, canShow }:
  Pick<RootProps, 'SessionProvider' | 'renderSlot' | 'mountView' | 'width' | 'viewportWidth' | 'canShow'>
  & { readonly view: SidebarSessionViewSnapshot; readonly visible: boolean }) {
  useLayoutEffect(() => mountView(view.reference), [mountView, view.reference])
  const active = visible && view.selected
  return <div className={css.session} hidden={!active} data-sidebar-right-session={view.sessionId}>
    <SessionProvider session={view.reference}>
      {renderSlot('rightbar.session', { width, viewportWidth, canShow, active, retainTab: view.retainTab })}
    </SessionProvider>
  </div>
}

/**
 * Keep independent Session subtrees and hide those outside the selected Conversation.
 * @param props - frame geometry, view targets and the authorized Session renderer.
 * @returns the foreground and retained background Sidebars.
 */
export function RightbarRoot({ usePanelInfo, useViews, useExternalHost, ...props }: RootProps) {
  const host = useExternalHost(value => value)
  const visible = usePanelInfo(info => info.activePanelId === null) || host !== undefined
  const views = useViews(value => value)
  const [hostWidth, setHostWidth] = useState(0)
  useLayoutEffect(() => {
    if (host === undefined) return
    const measure = (): void => { setHostWidth(host.element.clientWidth) }
    measure()
    const observer = new ResizeObserver(measure)
    observer.observe(host.element)
    return () => { observer.disconnect() }
  }, [host])
  const content = views.map(view => <SessionView key={view.sessionId} {...props}
    width={host ? hostWidth : props.width}
    viewportWidth={host ? Math.max(768, props.viewportWidth) : props.viewportWidth}
    canShow={host ? true : props.canShow}
    view={view} visible={visible} />)
  return host === undefined ? <>{content}</> : createPortal(content, host.element)
}
