/**
 * Server-only issuer capabilities for trusted in-process producers.
 * These tokens prevent accidental claim misuse. They are not a boundary
 * against hostile in-process plugins, which already have filesystem access.
 */
export const apiSessionControllerIssuer = Object.freeze({})
export const dshRoomLeaseIssuer = Object.freeze({})
